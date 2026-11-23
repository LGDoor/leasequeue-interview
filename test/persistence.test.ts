import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { describe, test } from "node:test";
import { promisify } from "node:util";
import { leaseRef, openQueue } from "../src/index.ts";
import type { EnqueueResult, JsonObject, QueueEvent } from "../src/index.ts";
import { claim, disk, hasCode, input, memory } from "./helpers.ts";

function eventSummary(events: QueueEvent[]) {
  return events.map(({ name, occurredAt, details }) => ({ name, occurredAt, details }));
}

const runFile = promisify(execFile);
const library = new URL(`../src/index.${import.meta.url.endsWith(".ts") ? "ts" : "js"}`, import.meta.url);

async function runInProcess<Result>(filename: string, source: string, ...args: string[]): Promise<Result> {
  const program = `
    import { openQueue, leaseRef } from ${JSON.stringify(library.href)};
    const [filename, ...args] = process.argv.slice(1);
    const queue = openQueue(filename, { clock: { now: () => 10000 } });
    try {
      ${source}
    } finally {
      queue.close();
    }
  `;
  const { stdout } = await runFile(process.execPath, [
    "--disable-warning=ExperimentalWarning", "--input-type=module", "--eval", program, filename, ...args,
  ], { timeout: 8_000 });
  return JSON.parse(stdout) as Result;
}

describe("durable state", () => {
  test("reopening preserves job fields, settings, and journal order", (context) => {
    const { open, clock } = disk(context);
    const producer = open();
    const { jobId } = producer.enqueue(input({
      idempotencyKey: "csv:day-15",
      payload: { tags: ["finance", "weekly"], totals: { units: 42 } },
      priority: 8,
      availableAt: clock.now() + 50,
    }));
    clock.advance(25);
    const settings = producer.setPaused("reports", true);
    const expected = producer.get(jobId);
    const history = producer.readEvents();
    producer.close();

    const reader = open();
    assert.deepEqual(reader.get(jobId), expected);
    assert.deepEqual(reader.getSettings("reports"), settings);
    assert.deepEqual(reader.readEvents(), history);
    assert.equal(reader.status("reports").delayed, 1);
    assert.deepEqual(reader.enqueue(input({ idempotencyKey: "csv:day-15" })), { jobId, inserted: false });
    assert.deepEqual(reader.get(jobId), expected);
  });

  test("closing a producer does not interrupt workers connected to the same file", (context) => {
    const { open } = disk(context);
    const producer = open();
    const worker = open();
    const { jobId } = producer.enqueue(input());
    producer.close();

    const job = claim(worker);
    assert.equal(job.id, jobId);
    assert.equal(worker.complete(leaseRef(job)), true);
    assert.equal(open().get(jobId)?.status, "succeeded");
  });

  test("durably preserves an active attempt across connection shutdown", (context) => {
    const { open, clock } = disk(context);
    const oldClient = open();
    const { jobId } = oldClient.enqueue(input());
    const job = claim(oldClient, { leaseMs: 20 });
    oldClient.close();
    clock.advance(10);

    const reader = open();
    assert.deepEqual(reader.get(jobId), job);
    assert.equal(reader.status("reports").expired, 0);
    assert.equal(reader.claim({ queue: "reports", workerId: "second" }), null);
    assert.deepEqual(reader.reclaimExpired(), { requeued: [], dead: [] });
  });

  test("a separate in-memory client does not see another client's data", (context) => {
    const { queue } = memory(context);
    const other = openQueue(":memory:");
    context.after(() => other.close());
    const { jobId } = queue.enqueue(input());
    queue.setPaused("reports", true);

    assert.equal(other.get(jobId), null);
    assert.equal(other.getSettings("reports").paused, false);
    assert.deepEqual(other.readEvents(), []);
  });

  test("a terminal attempt can be retried after a process restart", (context) => {
    const { open, clock } = disk(context);
    const worker = open();
    const { jobId } = worker.enqueue(input({ maxAttempts: 1, idempotencyKey: "delivery-5" }));
    const job = claim(worker);
    assert.deepEqual(worker.fail(leaseRef(job), { message: "connection refused" }), { outcome: "dead" });
    worker.close();

    const operator = open();
    const retried = operator.retryDead(jobId, { maxAttempts: 4, availableAt: clock.now() + 500 });
    assert.equal(retried?.attempts, 0);
    assert.equal(retried?.maxAttempts, 4);
    assert.equal(retried?.finishedAt, null);
    assert.equal(retried?.idempotencyKey, "delivery-5");
    assert.equal(retried?.lastError, null);
    assert.equal(operator.status("reports").delayed, 1);
    assert.deepEqual(operator.readEvents({ jobId }).map(({ name }) => name), [
      "enqueued", "claimed", "dead", "retried",
    ]);
  });
});

describe("database setup and transaction boundaries", () => {
  test("upgrades an earlier database without rewriting existing jobs", (context) => {
    const { open, inspect } = disk(context);
    const initial = open();
    const { jobId } = initial.enqueue(input({ priority: 9, idempotencyKey: "archive" }));
    const before = initial.get(jobId);
    initial.close();

    const database = inspect();
    database.exec(`
      DROP TABLE queue_events;
      DROP TABLE queue_settings;
      DROP INDEX jobs_queue;
      DROP INDEX jobs_queue_state;
      PRAGMA user_version = 1;
    `);

    const migrated = open();
    assert.deepEqual(migrated.get(jobId), before);
    assert.deepEqual(migrated.getSettings("reports"), {
      queue: "reports", paused: false, updatedAt: null,
    });
    assert.deepEqual(migrated.readEvents(), []);

    const job = claim(migrated);
    assert.equal(job.id, jobId);
    assert.deepEqual(migrated.readEvents().map(({ name }) => name), ["claimed"]);
    assert.ok(Number(database.prepare("PRAGMA user_version").get()?.user_version) > 1);
  });

  test("rejects a database that is newer than this library without downgrading it", (context) => {
    const { open, inspect } = disk(context);
    const database = inspect();
    database.exec("CREATE TABLE sentinel (value TEXT); INSERT INTO sentinel VALUES ('keep'); PRAGMA user_version = 9000");

    assert.throws(() => open(), hasCode("invalid-schema", /9000/));
    assert.equal(database.prepare("SELECT value FROM sentinel").get()?.value, "keep");
    assert.equal(database.prepare("PRAGMA user_version").get()?.user_version, 9000);
    assert.equal(database.prepare("SELECT name FROM sqlite_master WHERE name = 'jobs'").get(), undefined);
  });

  test("rolls back an incomplete migration and leaves its previous version intact", (context) => {
    const { open, inspect } = disk(context);
    const database = inspect();
    database.exec("CREATE TABLE queue_events (unrelated TEXT)");

    assert.throws(() => open(), /queue_events.*already exists/);
    assert.equal(database.prepare("PRAGMA user_version").get()?.user_version, 0);
    assert.equal(database.prepare("SELECT name FROM sqlite_master WHERE name = 'jobs'").get(), undefined);

    database.exec("DROP TABLE queue_events");
    const recovered = open();
    assert.equal(recovered.enqueue(input()).inserted, true);
  });

  test("a journal write failure rolls back the entire producer batch", (context) => {
    const { open, inspect } = disk(context);
    const writer = open();
    const observer = open();
    const database = inspect();
    database.exec(`
      CREATE TRIGGER reject_selected_job_event BEFORE INSERT ON queue_events
      WHEN json_extract(NEW.details_json, '$.kind') = 'reject'
      BEGIN
        SELECT RAISE(ABORT, 'test journal rejection');
      END;
    `);

    assert.throws(() => writer.enqueueMany([
      input({ idempotencyKey: "candidate-a" }),
      input({ idempotencyKey: "candidate-b", kind: "reject" }),
    ]), /test journal rejection/);
    assert.deepEqual(observer.list({ queue: "reports" }), { jobs: [], nextCursor: null });
    assert.deepEqual(observer.readEvents(), []);

    database.exec("DROP TRIGGER reject_selected_job_event");
    assert.equal(writer.enqueue(input({ idempotencyKey: "candidate-a" })).inserted, true);
    assert.equal(observer.readEvents().length, 1);
  });

  test("a rejected claim event leaves the job available and the attempt unspent", (context) => {
    const { open, inspect } = disk(context);
    const queue = open();
    const { jobId } = queue.enqueue(input());
    const database = inspect();
    database.exec(`
      CREATE TRIGGER reject_claim_event BEFORE INSERT ON queue_events
      WHEN NEW.event_name = 'claimed'
      BEGIN
        SELECT RAISE(ABORT, 'test claim rejection');
      END;
    `);

    assert.throws(() => queue.claim({ queue: "reports", workerId: "test-worker" }), /test claim rejection/);
    assert.equal(queue.get(jobId)?.status, "queued");
    assert.equal(queue.get(jobId)?.attempts, 0);
    assert.equal(queue.get(jobId)?.lease, null);

    database.exec("DROP TRIGGER reject_claim_event");
    assert.equal(claim(queue).attempts, 1);
  });

  test("recognizes lock contention and can write once the other transaction ends", (context) => {
    const { open, inspect } = disk(context);
    const queue = open({ busyTimeoutMs: 0 });
    const database = inspect();
    database.exec("BEGIN IMMEDIATE");

    try {
      assert.throws(() => queue.enqueue(input()), /database is locked/);
      assert.equal(queue.status("reports").counts.queued, 0);
    } finally {
      database.exec("ROLLBACK");
    }

    assert.equal(queue.enqueue(input()).inserted, true);
    assert.equal(queue.status("reports").counts.queued, 1);
  });
});

describe("independent Node processes", () => {
  test("serializes concurrent inserts from separate producers", async (context) => {
    const { filename, open } = disk(context);
    const producer = `
      const [submittedBy] = args;
      const records = Array.from({ length: 12 }, (_, index) => ({
        queue: "reports", kind: "export", payload: { submittedBy, index },
      }));
      console.log(JSON.stringify(queue.enqueueMany(records)));
    `;

    const [first, second] = await Promise.all([
      runInProcess<EnqueueResult[]>(filename, producer, "first"),
      runInProcess<EnqueueResult[]>(filename, producer, "second"),
    ]);

    const observer = open();
    const jobs = observer.list({ queue: "reports" }).jobs;
    assert.equal(jobs.length, 24);
    assert.equal(new Set(jobs.map(({ id }) => id)).size, 24);
    assert.ok([...first, ...second].every(({ inserted }) => inserted));
    assert.deepEqual(new Set(jobs.map(({ id }) => id)), new Set([...first, ...second].map(({ jobId }) => jobId)));
    assert.equal(observer.readEvents().length, 24);
  });

  test("two independent workers can drain a shared queue without processing a job twice", async (context) => {
    const { filename, open } = disk(context);
    const queue = open();
    const submitted = queue.enqueueMany(Array.from({ length: 30 }, () => input()));
    const worker = `
      const [workerId] = args;
      const completed = [];
      let job;
      while ((job = queue.claim({ queue: "reports", workerId }))) {
        if (!queue.complete(leaseRef(job))) throw new Error("unexpected loss of active job");
        completed.push(job.id);
      }
      console.log(JSON.stringify(completed));
    `;

    const completed = (await Promise.all([
      runInProcess<string[]>(filename, worker, "worker-east"),
      runInProcess<string[]>(filename, worker, "worker-west"),
    ])).flat();

    assert.equal(completed.length, submitted.length);
    assert.deepEqual(new Set(completed), new Set(submitted.map(({ jobId }) => jobId)));
    assert.equal(queue.status("reports").counts.succeeded, submitted.length);
    assert.equal(queue.status("reports").counts.running, 0);
    for (const { jobId } of submitted) {
      assert.equal(queue.get(jobId)?.attempts, 1);
      assert.deepEqual(queue.readEvents({ jobId }).map(({ name }) => name), ["enqueued", "claimed", "succeeded"]);
    }
  });

  test("applies queue settings written in another process", async (context) => {
    const { filename, open } = disk(context);
    const queue = open();
    const { jobId } = queue.enqueue(input());

    await runInProcess(filename, `
      queue.setPaused("reports", true);
      console.log(JSON.stringify(queue.getSettings("reports")));
    `);

    assert.equal(queue.getSettings("reports").paused, true);
    assert.equal(queue.claim({ queue: "reports", workerId: "parent-worker" }), null);

    await runInProcess(filename, `
      queue.setPaused("reports", false);
      console.log(JSON.stringify(queue.getSettings("reports")));
    `);

    assert.equal(queue.getSettings("reports").paused, false);
    assert.equal(claim(queue).id, jobId);
  });
});

describe("event journal", () => {
  test("records details for a completed workflow without embedding the payload or token", (context) => {
    const { queue, clock } = memory(context, 2_000);
    const { jobId } = queue.enqueue(input({ priority: 4, payload: { internalId: "finance-19" } }));
    clock.advance(10);
    const job = claim(queue, { leaseMs: 50 });
    clock.advance(10);
    queue.renew(leaseRef(job), 100);
    clock.advance(10);
    queue.complete(leaseRef(job));

    const events = queue.readEvents({ jobId });
    assert.deepEqual(eventSummary(events), [
      { name: "enqueued", occurredAt: 2_000, details: { kind: "export", priority: 4, availableAt: 2_000 } },
      { name: "claimed", occurredAt: 2_010, details: { workerId: "worker-one", expiresAt: 2_060, attempt: 1 } },
      { name: "lease-renewed", occurredAt: 2_020, details: { workerId: "worker-one", expiresAt: 2_120 } },
      { name: "succeeded", occurredAt: 2_030, details: { attempt: 1 } },
    ]);
    const serialized = JSON.stringify(events);
    assert.ok(!serialized.includes(job.lease.token));
    assert.ok(!serialized.includes("finance-19"));
  });

  test("uses an exclusive sequence cursor even when events share a timestamp", (context) => {
    const { queue } = memory(context);
    const jobs = queue.enqueueMany([input(), input(), input(), input(), input()]);
    const first = queue.readEvents({ limit: 2 });
    const second = queue.readEvents({ after: first[1].sequence, limit: 2 });
    const third = queue.readEvents({ after: second[1].sequence, limit: 2 });
    const all = [...first, ...second, ...third];

    assert.deepEqual(all.map(({ jobId }) => jobId), jobs.map(({ jobId }) => jobId));
    assert.equal(new Set(all.map(({ occurredAt }) => occurredAt)).size, 1);
    assert.deepEqual(queue.readEvents({ after: third[0].sequence }), []);
  });

  test("filters queue and job together and excludes queue events for a job filter", (context) => {
    const { queue } = memory(context);
    const first = queue.enqueue(input());
    const second = queue.enqueue(input({ queue: "notifications" }));
    queue.setPaused("reports", true);
    queue.cancel(first.jobId);

    assert.deepEqual(queue.readEvents({ queue: "reports" }).map(({ name, jobId }) => [name, jobId]), [
      ["enqueued", first.jobId], ["queue-paused", null], ["cancelled", first.jobId],
    ]);
    assert.deepEqual(queue.readEvents({ jobId: first.jobId }).map(({ name }) => name), ["enqueued", "cancelled"]);
    assert.deepEqual(queue.readEvents({ queue: "reports", jobId: second.jobId }), []);
  });

  test("reflects event sequence rather than sorting by a changing clock", (context) => {
    const { queue, clock } = memory(context, 500);
    const first = queue.enqueue(input());
    clock.set(400);
    const second = queue.enqueue(input());
    const events = queue.readEvents();

    assert.deepEqual(events.map(({ jobId }) => jobId), [first.jobId, second.jobId]);
    assert.deepEqual(events.map(({ occurredAt }) => occurredAt), [500, 400]);
    assert.ok(events[1].sequence > events[0].sequence);
  });

  test("prunes only matching old events while preserving job state and cursor continuity", (context) => {
    const { queue, clock } = memory(context, 1_000);
    const oldReport = queue.enqueue(input());
    const notification = queue.enqueue(input({ queue: "notifications" }));
    const cursor = queue.readEvents().at(-1)?.sequence;
    clock.set(1_100);
    const recentReport = queue.enqueue(input());

    assert.equal(queue.pruneEvents({ before: 1_100, queue: "reports" }), 1);
    assert.deepEqual(queue.readEvents().map(({ jobId }) => jobId), [notification.jobId, recentReport.jobId]);
    assert.deepEqual(queue.readEvents({ after: cursor }).map(({ jobId }) => jobId), [recentReport.jobId]);
    assert.equal(queue.get(oldReport.jobId)?.status, "queued");
    assert.equal(queue.pruneEvents({ before: 1_000 }), 0);
    assert.equal(queue.pruneEvents({ before: 1_101 }), 2);

    const next = queue.enqueue(input());
    assert.deepEqual(queue.readEvents({ after: cursor }).map(({ jobId }) => jobId), [next.jobId]);
  });

  test("deduplicates no-op setting changes without refreshing their timestamp", (context) => {
    const { queue, clock } = memory(context, 300);
    assert.deepEqual(queue.setPaused("reports", false), { queue: "reports", paused: false, updatedAt: null });
    const paused = queue.setPaused("reports", true);
    clock.advance(50);
    assert.deepEqual(queue.setPaused("reports", true), paused);
    const resumed = queue.setPaused("reports", false);
    assert.equal(resumed.updatedAt, 350);

    assert.deepEqual(eventSummary(queue.readEvents()), [
      { name: "queue-paused", occurredAt: 300, details: {} },
      { name: "queue-resumed", occurredAt: 350, details: {} },
    ]);
  });

  test("returns new details objects on independent reads", (context) => {
    const { queue } = memory(context);
    const { jobId } = queue.enqueue(input({ priority: 7 }));
    const original = queue.readEvents({ jobId })[0];
    const snapshot: JsonObject = original.details;
    snapshot.priority = 100;
    snapshot.note = "local only";

    const reread = queue.readEvents({ jobId })[0];
    assert.equal(reread.details.priority, 7);
    assert.equal(reread.details.note, undefined);
  });
});

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { describe, test } from "node:test";
import { leaseRef } from "../src/index.ts";
import { claim, disk, hasCode, input, memory } from "./helpers.ts";

describe("enqueue", () => {
  test("stores a snapshot of the supplied payload and options", (t) => {
    const { queue, clock } = memory(t, 5_000);
    const payload = { rows: ["first", "second"], config: { includeHeader: true } };

    const result = queue.enqueue(input({
      kind: "render-table",
      payload,
      priority: 4,
      availableAt: 5_040,
      maxAttempts: 5,
      retryPolicy: { initialDelayMs: 10, maxDelayMs: 50, multiplier: 1.5 },
    }));

    payload.rows.reverse();
    payload.config.includeHeader = false;
    assert.equal(result.inserted, true);

    assert.deepEqual(queue.get(result.jobId), {
      id: result.jobId,
      queue: "reports",
      kind: "render-table",
      payload: { rows: ["first", "second"], config: { includeHeader: true } },
      status: "queued",
      priority: 4,
      availableAt: 5_040,
      attempts: 0,
      maxAttempts: 5,
      retryPolicy: { initialDelayMs: 10, maxDelayMs: 50, multiplier: 1.5 },
      idempotencyKey: null,
      lease: null,
      lastError: null,
      createdAt: clock.now(),
      updatedAt: clock.now(),
      finishedAt: null,
    });
  });

  test("does not deduplicate calls without a key", (t) => {
    const { queue } = memory(t);
    const first = queue.enqueue(input());
    const second = queue.enqueue(input());

    assert.ok(first.inserted);
    assert.ok(second.inserted);
    assert.notEqual(first.jobId, second.jobId);
    assert.deepEqual(queue.list({ queue: "reports" }).jobs.map(({ id }) => id), [
      first.jobId,
      second.jobId,
    ]);
  });

  test("returns the original job without modifying its data on replay", (t) => {
    const { queue, clock } = memory(t, 4_000);
    const first = queue.enqueue(input({
      payload: { revision: 1 },
      idempotencyKey: "report-04",
      maxAttempts: 2,
    }));
    clock.advance(12);

    const replay = queue.enqueue(input({
      payload: { revision: 2 },
      idempotencyKey: "report-04",
      priority: 9,
      maxAttempts: 8,
    }));

    assert.deepEqual(replay, { jobId: first.jobId, inserted: false });
    const saved = queue.get(first.jobId);
    assert.ok(saved);
    assert.deepEqual(saved.payload, { revision: 1 });
    assert.equal(saved.priority, 0);
    assert.equal(saved.maxAttempts, 2);
    assert.equal(saved.updatedAt, 4_000);
    assert.equal(queue.readEvents({ jobId: first.jobId }).length, 1);
  });

  test("shares duplicate detection across connections to the same database", (t) => {
    const fixture = disk(t);
    const producer = fixture.open();
    const replica = fixture.open();
    const first = producer.enqueue(input({ idempotencyKey: "weekly" }));
    const duplicate = replica.enqueue(input({ idempotencyKey: "weekly" }));

    assert.deepEqual(duplicate, { jobId: first.jobId, inserted: false });
    assert.equal(replica.status("reports").ready, 1);
  });

  test("replays a recent submission without waiting for a writer lock", (t) => {
    const { filename, open } = disk(t);
    const producer = open({ busyTimeoutMs: 0 });
    const request = input({ idempotencyKey: "nightly-export" });
    const first = producer.enqueue(request);
    const writer = new DatabaseSync(filename);

    try {
      writer.exec("BEGIN IMMEDIATE");
      assert.deepEqual(producer.enqueue(request), { jobId: first.jobId, inserted: false });
      assert.throws(() => producer.enqueue(input()), /database is locked/);
    } finally {
      writer.close();
    }

    assert.equal(producer.status("reports").ready, 1);
    assert.equal(producer.readEvents({ jobId: first.jobId }).length, 1);
  });

  test("keeps idempotency associations after losing the original worker", (t) => {
    const { queue, clock } = memory(t);
    const submitted = queue.enqueue(input({ idempotencyKey: "run-09" }));
    const leased = claim(queue, { leaseMs: 10 });
    assert.equal(leased.id, submitted.jobId);
    clock.advance(10);
    queue.reclaimExpired();

    assert.deepEqual(queue.enqueue(input({ idempotencyKey: "run-09" })), {
      jobId: submitted.jobId,
      inserted: false,
    });
    assert.equal(queue.get(submitted.jobId)?.attempts, 1);
  });

  test("treats punctuation and letter case in keys as literal data", (t) => {
    const { queue } = memory(t);
    const keys = ["v1:report|US", "v1:report|us", " v1:report|US ", "quarter's end"];
    const results = keys.map((idempotencyKey) => queue.enqueue(input({ idempotencyKey })));

    assert.equal(new Set(results.map(({ jobId }) => jobId)).size, keys.length);
    assert.deepEqual(queue.list({ queue: "reports" }).jobs.map(({ idempotencyKey }) => idempotencyKey), keys);
  });
});

describe("batch submission", () => {
  test("returns results in input order, including duplicates", (t) => {
    const { queue } = memory(t);
    const preexisting = queue.enqueue(input({ idempotencyKey: "existing" }));

    const results = queue.enqueueMany([
      input({ idempotencyKey: "existing" }),
      input({ idempotencyKey: "new" }),
      input({ idempotencyKey: "new" }),
      input(),
    ]);

    assert.equal(results.length, 4);
    assert.deepEqual(results[0], { jobId: preexisting.jobId, inserted: false });
    assert.deepEqual(results[2], { jobId: results[1].jobId, inserted: false });
    assert.equal(results[1].inserted, true);
    assert.equal(results[3].inserted, true);
    assert.deepEqual(queue.readEvents().map(({ jobId }) => jobId), [
      preexisting.jobId,
      results[1].jobId,
      results[3].jobId,
    ]);
  });

  test("does not save a prefix of a batch with invalid input", (t) => {
    const { queue } = memory(t);

    assert.throws(() => queue.enqueueMany([
      input({ kind: "first" }),
      input({ priority: 20_000 }),
      input({ kind: "last" }),
    ]), hasCode("invalid-input", /priority/));

    assert.deepEqual(queue.list({ queue: "reports" }), { jobs: [], nextCursor: null });
    assert.deepEqual(queue.readEvents(), []);
  });

  test("accepts empty batches as a no-op", (t) => {
    const { queue } = memory(t);
    assert.deepEqual(queue.enqueueMany([]), []);
    assert.equal(queue.status("reports").counts.queued, 0);
  });
});

describe("claim order", () => {
  test("skips future jobs even if their priority is higher", (t) => {
    const { queue, clock } = memory(t, 1_000);
    const [scheduled, low, high] = queue.enqueueMany([
      input({ availableAt: 1_010, priority: 50 }),
      input({ priority: -2 }),
      input({ priority: 2 }),
    ]);

    assert.equal(claim(queue).id, high.jobId);
    assert.equal(claim(queue).id, low.jobId);
    assert.equal(queue.claim({ queue: "reports", workerId: "worker-one" }), null);
    clock.advance(10);
    assert.equal(claim(queue).id, scheduled.jobId);
  });

  test("breaks ties by availability and then insertion order", (t) => {
    const { queue } = memory(t, 1_000);
    const [late, first, next] = queue.enqueueMany([
      input({ availableAt: 900 }),
      input({ availableAt: 800 }),
      input({ availableAt: 800 }),
    ]);

    const picked = [claim(queue).id, claim(queue).id, claim(queue).id];
    assert.deepEqual(picked, [first.jobId, next.jobId, late.jobId]);
  });

  test("never claims work from a different queue", (t) => {
    const { queue } = memory(t);
    const only = queue.enqueue(input({ queue: "billing", priority: 100 }));

    assert.equal(queue.claim({ queue: "reports", workerId: "worker-one" }), null);
    assert.equal(claim(queue, { queue: "billing" }).id, only.jobId);
  });

  test("does not hand a currently running job to another connection", (t) => {
    const fixture = disk(t);
    const firstConnection = fixture.open();
    const nextConnection = fixture.open();
    firstConnection.enqueue(input());

    const first = claim(firstConnection, { workerId: "primary" });
    assert.equal(nextConnection.claim({ queue: "reports", workerId: "secondary" }), null);
    assert.equal(nextConnection.get(first.id)?.lease?.workerId, "primary");
  });
});

describe("job list and snapshot", () => {
  test("page cursors remain stable when new jobs are appended", (t) => {
    const { queue } = memory(t);
    const [first, second, third] = queue.enqueueMany([input(), input(), input()]);
    const start = queue.list({ queue: "reports", limit: 2 });

    assert.deepEqual(start.jobs.map(({ id }) => id), [first.jobId, second.jobId]);
    assert.ok(start.nextCursor);
    const fourth = queue.enqueue(input());

    const tail = queue.list({ queue: "reports", after: start.nextCursor, limit: 2 });
    assert.deepEqual(tail.jobs.map(({ id }) => id), [third.jobId, fourth.jobId]);
    assert.equal(tail.nextCursor, null);
  });

  test("returns only the selected queue and state", (t) => {
    const { queue } = memory(t);
    const [first, second, other] = queue.enqueueMany([
      input(),
      input(),
      input({ queue: "cleanup" }),
    ]);
    queue.cancel(first.jobId);
    queue.cancel(other.jobId);

    assert.deepEqual(queue.list({ queue: "reports", status: "cancelled" }).jobs.map(({ id }) => id), [
      first.jobId,
    ]);
    assert.deepEqual(queue.list({ queue: "reports", status: "queued" }).jobs.map(({ id }) => id), [
      second.jobId,
    ]);
  });

  test("returns an empty page for a cursor past the end", (t) => {
    const { queue } = memory(t);
    queue.enqueue(input());
    assert.deepEqual(queue.list({ queue: "reports", after: 99 }), { jobs: [], nextCursor: null });
    assert.equal(queue.get("unknown-job"), null);
  });

  test("summarizes ready, delayed, running and terminal work separately", (t) => {
    const { queue, clock } = memory(t, 1_000);
    const [done, cancelled, running, delayed, ready] = queue.enqueueMany([
      input({ priority: 8 }),
      input({ priority: 6 }),
      input({ priority: 4 }),
      input({ availableAt: 1_100 }),
      input(),
    ]);

    const completedLease = claim(queue, { leaseMs: 20 });
    assert.equal(completedLease.id, done.jobId);
    assert.ok(queue.complete(leaseRef(completedLease)));
    assert.ok(queue.cancel(cancelled.jobId));
    assert.equal(claim(queue, { leaseMs: 10 }).id, running.jobId);
    clock.advance(10);

    assert.deepEqual(queue.status("reports"), {
      queue: "reports",
      paused: false,
      counts: { queued: 2, running: 1, succeeded: 1, dead: 0, cancelled: 1 },
      ready: 1,
      delayed: 1,
      expired: 1,
      nextAvailableAt: 1_000,
    });
    assert.equal(queue.get(ready.jobId)?.status, "queued");
    assert.equal(queue.get(delayed.jobId)?.availableAt, 1_100);
  });

  test("reports empty queues with zero counts and no next job", (t) => {
    const { queue } = memory(t);
    queue.enqueue(input());
    assert.deepEqual(queue.status("other"), {
      queue: "other",
      paused: false,
      counts: { queued: 0, running: 0, succeeded: 0, dead: 0, cancelled: 0 },
      ready: 0,
      delayed: 0,
      expired: 0,
      nextAvailableAt: null,
    });
  });
});

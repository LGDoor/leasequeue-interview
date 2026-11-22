import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { leaseRef, openQueue, poll, RetryAfterError, Worker } from "../src/index.ts";
import type { EnqueueInput, JsonValue, QueueClient, WorkerOptions } from "../src/index.ts";
import { claim, hasCode, input, memory } from "./helpers.ts";

const workerOptions: WorkerOptions = {
  queue: "reports",
  workerId: "test-worker",
  handlers: { export() {} },
};

type BadEnqueue = {
  label: string;
  override: Partial<EnqueueInput>;
  field: RegExp;
};

const badEnqueues: BadEnqueue[] = [
  { label: "empty queue", override: { queue: "" }, field: /queue/ },
  { label: "leading punctuation in queue", override: { queue: "-reports" }, field: /queue/ },
  { label: "whitespace in kind", override: { kind: "send mail" }, field: /job kind/ },
  { label: "overlong kind", override: { kind: "x".repeat(101) }, field: /job kind/ },
  { label: "blank key", override: { idempotencyKey: "  " }, field: /idempotency key/ },
  { label: "NUL in key", override: { idempotencyKey: "week\0one" }, field: /idempotency key/ },
  { label: "overlong key", override: { idempotencyKey: "k".repeat(201) }, field: /idempotency key/ },
  { label: "fractional priority", override: { priority: 1.5 }, field: /priority/ },
  { label: "priority below minimum", override: { priority: -10_001 }, field: /priority/ },
  { label: "priority above maximum", override: { priority: 10_001 }, field: /priority/ },
  { label: "negative availability", override: { availableAt: -1 }, field: /availableAt/ },
  { label: "unsafe availability", override: { availableAt: Number.MAX_SAFE_INTEGER + 1 }, field: /availableAt/ },
  { label: "infinite availability", override: { availableAt: Infinity }, field: /availableAt/ },
  { label: "zero attempt budget", override: { maxAttempts: 0 }, field: /maxAttempts/ },
  { label: "excessive attempt budget", override: { maxAttempts: 101 }, field: /maxAttempts/ },
  { label: "negative backoff start", override: { retryPolicy: { initialDelayMs: -1 } }, field: /initial retry delay/ },
  { label: "cap below backoff start", override: { retryPolicy: { maxDelayMs: 500 } }, field: /maximum retry delay/ },
  { label: "nonfinite backoff multiplier", override: { retryPolicy: { multiplier: NaN } }, field: /retry multiplier/ },
  { label: "decreasing backoff", override: { retryPolicy: { multiplier: 0.5 } }, field: /retry multiplier/ },
];

describe("producer inputs", () => {
  for (const { label, override, field } of badEnqueues) {
    test(`rejects ${label} without creating a job`, (context) => {
      const { queue } = memory(context);

      assert.throws(() => queue.enqueue(input(override)), hasCode("invalid-input", field));
      assert.deepEqual(queue.list({ queue: "reports" }), { jobs: [], nextCursor: null });
      assert.deepEqual(queue.readEvents(), []);
    });
  }

  test("accepts inclusive bounds on fields", (context) => {
    const { queue } = memory(context);
    const queueName = "a".repeat(100);
    const kind = "b".repeat(100);
    const [lowest, highest] = queue.enqueueMany([
      input({ queue: queueName, kind, priority: -10_000, maxAttempts: 1, availableAt: 0 }),
      input({ queue: queueName, kind, priority: 10_000, maxAttempts: 100, availableAt: Number.MAX_SAFE_INTEGER }),
    ]);

    assert.equal(queue.get(lowest.jobId)?.priority, -10_000);
    assert.equal(queue.get(lowest.jobId)?.availableAt, 0);
    assert.equal(queue.get(highest.jobId)?.priority, 10_000);
    assert.equal(queue.get(highest.jobId)?.availableAt, Number.MAX_SAFE_INTEGER);
    assert.equal(queue.status(queueName).delayed, 1);
  });

  test("preserves names and meaningful spaces in keys without normalizing them", (context) => {
    const { queue } = memory(context);
    const original = queue.enqueue(input({ queue: "Reports-v2.EU_1", idempotencyKey: "daily" }));
    const separate = queue.enqueue(input({ queue: "Reports-v2.EU_1", idempotencyKey: " daily " }));

    assert.notEqual(original.jobId, separate.jobId);
    assert.equal(queue.get(separate.jobId)?.idempotencyKey, " daily ");
    assert.equal(queue.status("Reports-v2.EU_1").ready, 2);
    assert.equal(queue.status("reports-v2.EU_1").ready, 0);
  });

  test("rejects an oversized batch before changing any state", (context) => {
    const { queue } = memory(context);
    const tooMany = Array.from({ length: 201 }, () => input());
    assert.throws(() => queue.enqueueMany(tooMany), hasCode("invalid-input", /batch/));
    assert.deepEqual(queue.readEvents(), []);
    assert.equal(queue.status("reports").ready, 0);
  });

  test("accepts the largest batch and uses one clock snapshot", (context) => {
    let now = 0;
    const queue = openQueue(":memory:", { clock: { now: () => ++now } });
    context.after(() => queue.close());
    const jobs = queue.enqueueMany(Array.from({ length: 200 }, () => input()));
    const events = queue.readEvents({ limit: 250 });

    assert.equal(jobs.length, 200);
    assert.equal(now, 1);
    assert.equal(events.length, 200);
    assert.ok(events.every(({ occurredAt }) => occurredAt === 1));
  });
});

describe("JSON payloads", () => {
  const unsupported: { label: string; value: unknown }[] = [
    { label: "undefined", value: undefined },
    { label: "a Date", value: new Date(0) },
    { label: "a Map", value: new Map() },
    { label: "a bigint", value: 2n },
    { label: "a symbol", value: Symbol("symbol") },
    { label: "a function", value: () => 1 },
    { label: "infinity", value: Infinity },
    { label: "a sparse array", value: new Array<JsonValue>(2) },
    { label: "a nested non-JSON value", value: [{ amount: NaN }] },
  ];

  for (const { label, value } of unsupported) {
    test(`does not silently coerce ${label}`, (context) => {
      const { queue } = memory(context);
      assert.throws(() => queue.enqueue(input({ payload: value as JsonValue })), hasCode("invalid-input", /payload/));
    });
  }

  test("allows null and other scalar JSON values", (context) => {
    const { queue } = memory(context);
    const values = [null, true, false, 0, -20.5, "", "report"];

    const results = queue.enqueueMany(values.map((payload) => input({ payload })));
    assert.deepEqual(results.map(({ jobId }) => queue.get(jobId)?.payload), values);
  });

  test("allows shared subtrees without confusing them with cycles", (context) => {
    const { queue } = memory(context);
    const shared = { valid: true };
    const { jobId } = queue.enqueue(input({ payload: [shared, shared] }));

    assert.deepEqual(queue.get(jobId)?.payload, [{ valid: true }, { valid: true }]);
  });

  test("rejects an indirect cycle without writing a job", (context) => {
    const { queue } = memory(context);
    const root: JsonValue[] = [];
    root.push({ parent: root });

    assert.throws(() => queue.enqueue(input({ payload: root })), hasCode("invalid-input", /circular/));
    assert.equal(queue.status("reports").ready, 0);
  });

  test("measures serialized UTF-8 byte size rather than JavaScript string length", (context) => {
    const { queue } = memory(context);
    const atLimit = "é".repeat(31_999);
    const { jobId } = queue.enqueue(input({ payload: atLimit }));

    assert.equal(queue.get(jobId)?.payload, atLimit);
    assert.throws(() => queue.enqueue(input({ payload: `${atLimit}a` })), hasCode("invalid-input", /bytes/));
  });

  test("limits deeply nested payloads before serializing them", (context) => {
    const { queue } = memory(context);
    let nested: JsonValue = "leaf";
    for (let level = 0; level < 40; level += 1) nested = { value: nested };

    assert.equal(queue.enqueue(input({ payload: nested })).inserted, true);
    assert.throws(() => queue.enqueue(input({ payload: [nested] })), hasCode("invalid-input", /deeply nested/));
  });
});

describe("client misuse", () => {
  for (const filename of ["", " ", "a\0b"]) {
    test(`rejects invalid path ${JSON.stringify(filename)}`, () => {
      assert.throws(() => openQueue(filename), hasCode("invalid-input", /database path/));
    });
  }

  for (const busyTimeoutMs of [-1, 60_001, 2.5]) {
    test(`rejects invalid busy timeout ${busyTimeoutMs}`, () => {
      assert.throws(() => openQueue(":memory:", { busyTimeoutMs }), hasCode("invalid-input", /busy timeout/));
    });
  }

  test("reports closed uniformly after an idempotent close", (context) => {
    const { queue } = memory(context);
    assert.equal(queue.isClosed, false);
    queue.close();
    queue.close();
    assert.equal(queue.isClosed, true);

    const operations: (() => unknown)[] = [
      () => queue.enqueue(input()),
      () => queue.enqueueMany([]),
      () => queue.get("unknown"),
      () => queue.list({ queue: "reports" }),
      () => queue.status("reports"),
      () => queue.claim({ queue: "reports", workerId: "test-worker" }),
      () => queue.getSettings("reports"),
      () => queue.readEvents(),
      () => queue.pruneEvents({ before: 0 }),
    ];
    for (const operation of operations) assert.throws(operation, hasCode("closed"));
  });

  test("rejects invalid clock values on timed operations, but still permits inspection", (context) => {
    const { queue, clock } = memory(context);
    const { jobId } = queue.enqueue(input());

    for (const value of [-1, 1.1, NaN, Number.MAX_SAFE_INTEGER + 1]) {
      clock.set(value);
      assert.throws(() => queue.enqueue(input()), hasCode("invalid-input", /clock/));
      assert.throws(() => queue.status("reports"), hasCode("invalid-input", /clock/));
      assert.equal(queue.get(jobId)?.status, "queued");
    }
  });

  test("fails a claim cleanly when the requested expiry would overflow", (context) => {
    const { queue } = memory(context, Number.MAX_SAFE_INTEGER);
    const { jobId } = queue.enqueue(input());

    assert.throws(() => queue.claim({ queue: "reports", workerId: "test-worker", leaseMs: 1 }), hasCode("invalid-input", /time/));
    assert.equal(queue.get(jobId)?.attempts, 0);
    assert.equal(queue.get(jobId)?.status, "queued");
    assert.deepEqual(queue.readEvents({ jobId }).map(({ name }) => name), ["enqueued"]);
  });

  test("does not mutate a job when a requested transition has invalid input", (context) => {
    const { queue } = memory(context);
    queue.enqueue(input());
    const job = claim(queue);
    const reference = leaseRef(job);

    assert.throws(() => queue.renew(reference, 0), hasCode("invalid-input", /lease duration/));
    assert.throws(() => queue.fail(reference, { message: "" }), hasCode("invalid-input", /failure message/));
    assert.throws(() => queue.fail(reference, { message: "failure", retryAfterMs: -1 }), hasCode("invalid-input", /retry delay/));
    assert.throws(() => queue.cancel("\0"), hasCode("invalid-input", /job ID/));
    assert.deepEqual(queue.get(job.id), job);
    assert.deepEqual(queue.readEvents({ jobId: job.id }).map(({ name }) => name), ["enqueued", "claimed"]);
  });

  const badReads: { label: string; operation(queue: QueueClient): unknown; field: RegExp }[] = [
    { label: "job cursor", operation: (queue) => queue.list({ queue: "reports", after: -1 }), field: /cursor/ },
    { label: "job limit", operation: (queue) => queue.list({ queue: "reports", limit: 251 }), field: /page limit/ },
    { label: "event cursor", operation: (queue) => queue.readEvents({ after: 0.25 }), field: /event cursor/ },
    { label: "event limit", operation: (queue) => queue.readEvents({ limit: 0 }), field: /event limit/ },
    { label: "reclaim limit", operation: (queue) => queue.reclaimExpired({ limit: 251 }), field: /reclaim limit/ },
    { label: "prune threshold", operation: (queue) => queue.pruneEvents({ before: -1 }), field: /event cutoff/ },
  ];

  for (const { label, operation, field } of badReads) {
    test(`rejects invalid ${label}`, (context) => {
      const { queue } = memory(context);
      assert.throws(() => operation(queue), hasCode("invalid-input", field));
    });
  }
});

describe("worker options", () => {
  test("rejects a blank worker ID", (context) => {
    const { queue } = memory(context);
    assert.throws(() => new Worker(queue, { ...workerOptions, workerId: " " }), hasCode("invalid-input", /worker ID/));
  });

  test("rejects invalid handler names before the first claim", (context) => {
    const { queue } = memory(context);
    assert.throws(() => new Worker(queue, {
      ...workerOptions,
      handlers: { "not a job kind"() {} },
    }), hasCode("invalid-input", /job kind/));
  });

  test("rejects an invalid duration for an explicit retry signal", () => {
    for (const delay of [-1, 0.5, Infinity]) {
      assert.throws(() => new RetryAfterError("bad retry delay", delay), hasCode("invalid-input", /retry delay/));
    }
  });

  test("rejects invalid batch options without claiming work", async (context) => {
    const { queue } = memory(context);
    const { jobId } = queue.enqueue(input());
    const worker = new Worker(queue, workerOptions);

    await assert.rejects(worker.runBatch({ limit: 201 }), hasCode("invalid-input", /batch limit/));
    await assert.rejects(poll(worker, { batchLimit: 0 }), hasCode("invalid-input", /batch limit/));
    await assert.rejects(poll(worker, { idleDelayMs: 60_001 }), hasCode("invalid-input", /idle delay/));
    await assert.rejects(poll(worker, { maxIterations: 0 }), hasCode("invalid-input", /iteration limit/));
    assert.equal(queue.get(jobId)?.attempts, 0);
  });
});

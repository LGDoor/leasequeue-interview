import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { PermanentJobError, poll, RetryAfterError, Worker } from "../src/index.ts";
import type { BatchResult, JobContext, WorkerOptions } from "../src/index.ts";
import { deferred, disk, hasCode, input, memory } from "./helpers.ts";

const reportWorker = {
  queue: "reports",
  workerId: "reporter",
  handlers: { export() {} },
} satisfies WorkerOptions;

describe("handler execution", () => {
  test("returns idle without invoking any handler", async (context) => {
    const { queue } = memory(context);
    const worker = new Worker(queue, {
      ...reportWorker,
      handlers: {
        export() {
          assert.fail("no job was available");
        },
      },
    });

    assert.deepEqual(await worker.runOnce(), { outcome: "idle" });
    assert.deepEqual(queue.readEvents(), []);
  });

  test("passes a claimed snapshot to the matching handler", async (context) => {
    const { queue } = memory(context);
    const { jobId } = queue.enqueue(input({ kind: "render", payload: { format: "pdf" } }));
    const received: JobContext[] = [];
    const worker = new Worker(queue, {
      ...reportWorker,
      handlers: {
        export() {
          assert.fail("unexpected handler");
        },
        render(jobContext) {
          received.push(jobContext);
        },
      },
    });

    assert.deepEqual(await worker.runOnce(), { outcome: "succeeded", jobId });
    assert.equal(received.length, 1);
    assert.equal(received[0].job.id, jobId);
    assert.equal(received[0].job.status, "running");
    assert.equal(received[0].job.attempts, 1);
    assert.equal(received[0].job.lease.workerId, "reporter");
    assert.deepEqual(received[0].job.payload, { format: "pdf" });
    assert.equal(queue.get(jobId)?.status, "succeeded");
  });

  test("does not report success until an async handler has returned", async (context) => {
    const { open } = disk(context);
    const processing = open();
    const observer = open();
    const { jobId } = observer.enqueue(input());
    const started = deferred();
    const finish = deferred();
    const worker = new Worker(processing, {
      ...reportWorker,
      handlers: {
        async export() {
          started.resolve();
          await finish.promise;
        },
      },
    });

    const running = worker.runOnce();
    await started.promise;
    try {
      assert.equal(observer.get(jobId)?.status, "running");
      assert.deepEqual(observer.readEvents({ jobId }).map((event) => event.name), ["enqueued", "claimed"]);
    } finally {
      finish.resolve();
      await running;
    }

    assert.deepEqual(await running, { outcome: "succeeded", jobId });
    assert.equal(observer.get(jobId)?.status, "succeeded");
  });

  test("allows a processing handler to renew during lengthy work", async (context) => {
    const { queue, clock } = memory(context);
    const { jobId } = queue.enqueue(input());
    const worker = new Worker(queue, {
      ...reportWorker,
      leaseMs: 50,
      handlers: {
        export({ job, renew }) {
          clock.advance(40);
          const extended = renew(100);
          assert.equal(extended?.lease.expiresAt, clock.now() + 100);
          assert.equal(extended?.lease.token, job.lease.token);
          clock.advance(40);
        },
      },
    });

    assert.deepEqual(await worker.runOnce(), { outcome: "succeeded", jobId });
    assert.equal(queue.get(jobId)?.attempts, 1);
  });

  test("marks an unhandled kind dead without consuming the remaining budget", async (context) => {
    const { queue } = memory(context);
    const { jobId } = queue.enqueue(input({ kind: "unregistered", maxAttempts: 5 }));
    const worker = new Worker(queue, reportWorker);

    assert.deepEqual(await worker.runOnce(), { outcome: "dead", jobId });
    assert.equal(queue.get(jobId)?.attempts, 1);
    assert.equal(queue.get(jobId)?.lastError, "No handler for unregistered");
    assert.deepEqual(await worker.runOnce(), { outcome: "idle" });
  });

  test("preserves a permanent error message", async (context) => {
    const { queue } = memory(context);
    const { jobId } = queue.enqueue(input());
    const worker = new Worker(queue, {
      ...reportWorker,
      handlers: {
        async export() {
          throw new PermanentJobError("unsupported report format");
        },
      },
    });

    assert.deepEqual(await worker.runOnce(), { outcome: "dead", jobId });
    assert.equal(queue.get(jobId)?.lastError, "unsupported report format");
  });

  test("honors a handler's retry delay and leaves it delayed until due", async (context) => {
    const { queue, clock } = memory(context);
    const { jobId } = queue.enqueue(input({ retryPolicy: { initialDelayMs: 900 } }));
    const worker = new Worker(queue, {
      ...reportWorker,
      handlers: {
        export({ job }) {
          if (job.attempts === 1) throw new RetryAfterError("rate limited", 75);
        },
      },
    });

    assert.deepEqual(await worker.runOnce(), { outcome: "retry", jobId, availableAt: clock.now() + 75 });
    clock.advance(74);
    assert.deepEqual(await worker.runOnce(), { outcome: "idle" });
    clock.advance(1);
    assert.deepEqual(await worker.runOnce(), { outcome: "succeeded", jobId });
    assert.equal(queue.get(jobId)?.attempts, 2);
  });

  for (const scenario of [
    { label: "blank errors", value: new Error("   "), expected: "Job handler failed" },
    { label: "strings", value: "  temporary failure  ", expected: "temporary failure" },
    { label: "other thrown values", value: { reason: "not an Error" }, expected: "Job handler failed" },
    { label: "long errors", value: new Error("x".repeat(3_000)), expected: "x".repeat(2_048) },
  ]) {
    test(`normalizes ${scenario.label} with ordinary backoff`, async (context) => {
      const { queue, clock } = memory(context);
      const { jobId } = queue.enqueue(input({ retryPolicy: { initialDelayMs: 125 } }));
      const worker = new Worker(queue, {
        ...reportWorker,
        handlers: {
          export() {
            throw scenario.value;
          },
        },
      });

      assert.deepEqual(await worker.runOnce(), { outcome: "retry", jobId, availableAt: clock.now() + 125 });
      assert.equal(queue.get(jobId)?.lastError, scenario.expected);
    });
  }

  test("reports lost when an operator cancels active work", async (context) => {
    const { open } = disk(context);
    const queue = open();
    const operator = open();
    const { jobId } = queue.enqueue(input());
    const worker = new Worker(queue, {
      ...reportWorker,
      handlers: {
        export({ job }) {
          assert.equal(operator.cancel(job.id), true);
        },
      },
    });

    assert.deepEqual(await worker.runOnce(), { outcome: "lost", jobId });
    assert.equal(queue.get(jobId)?.status, "cancelled");
  });

  test("does not treat a closed client as an idle queue or a handler error", async (context) => {
    const { queue } = memory(context);
    const worker = new Worker(queue, reportWorker);
    queue.close();

    await assert.rejects(worker.runOnce(), hasCode("closed"));
  });
});

describe("bounded batches", () => {
  test("stops at the configured limit without claiming a further job", async (context) => {
    const { queue } = memory(context);
    const submitted = queue.enqueueMany([input(), input(), input()]);
    const worker = new Worker(queue, reportWorker);

    assert.deepEqual(await worker.runBatch({ limit: 2 }), {
      results: submitted.slice(0, 2).map(({ jobId }) => ({ outcome: "succeeded", jobId })),
      drained: false,
    });
    assert.equal(queue.get(submitted[2].jobId)?.attempts, 0);
    assert.deepEqual(await worker.runBatch({ limit: 2 }), {
      results: [{ outcome: "succeeded", jobId: submitted[2].jobId }],
      drained: true,
    });
  });

  test("counts failures as outcomes and keeps processing other jobs", async (context) => {
    const { queue, clock } = memory(context);
    const [retry, success, dead] = queue.enqueueMany([
      input({ kind: "retry" }),
      input(),
      input({ kind: "unhandled" }),
    ]);
    const worker = new Worker(queue, {
      ...reportWorker,
      handlers: {
        ...reportWorker.handlers,
        retry() {
          throw new Error("service unavailable");
        },
      },
    });

    assert.deepEqual(await worker.runBatch(), {
      results: [
        { outcome: "retry", jobId: retry.jobId, availableAt: clock.now() + 1_000 },
        { outcome: "succeeded", jobId: success.jobId },
        { outcome: "dead", jobId: dead.jobId },
      ],
      drained: true,
    });
  });

  test("finishes an active handler when aborted, but starts no further work", async (context) => {
    const { queue } = memory(context);
    const [first, second] = queue.enqueueMany([input(), input()]);
    const controller = new AbortController();
    const worker = new Worker(queue, {
      ...reportWorker,
      handlers: { export: () => controller.abort() },
    });

    assert.deepEqual(await worker.runBatch({ signal: controller.signal }), {
      results: [{ outcome: "succeeded", jobId: first.jobId }],
      drained: false,
    });
    assert.equal(queue.get(second.jobId)?.status, "queued");
    assert.deepEqual(await worker.runBatch({ signal: controller.signal }), { results: [], drained: false });
  });
});

describe("polling", () => {
  test("waits after observing an empty queue and picks up newly due work", async (context) => {
    const { queue, clock } = memory(context);
    const [ready, later] = queue.enqueueMany([
      input(),
      input({ availableAt: clock.now() + 40 }),
    ]);
    const worker = new Worker(queue, reportWorker);
    const waits: number[] = [];
    const batches: BatchResult[] = [];

    const result = await poll(worker, {
      maxIterations: 4,
      batchLimit: 1,
      idleDelayMs: 40,
      onBatch: (batch) => { batches.push(batch); },
      async wait(duration) {
        waits.push(duration);
        clock.advance(duration);
      },
    });

    assert.deepEqual(result, { stopped: "limit", iterations: 4, jobs: 2, idleIterations: 2 });
    assert.deepEqual(waits, [40]);
    assert.deepEqual(batches.map(({ results }) => results.map(({ jobId }) => jobId)), [
      [ready.jobId], [], [later.jobId], [],
    ]);
  });

  test("waits after a partial batch that observed idle", async (context) => {
    const { queue } = memory(context);
    queue.enqueue(input());
    const worker = new Worker(queue, reportWorker);
    const waits: number[] = [];

    const result = await poll(worker, {
      maxIterations: 2,
      batchLimit: 5,
      async wait(duration) { waits.push(duration); },
    });

    assert.deepEqual(result, { stopped: "limit", iterations: 2, jobs: 1, idleIterations: 1 });
    assert.deepEqual(waits, [100]);
  });

  test("passes the abort signal to an injected wait and stops before another batch", async (context) => {
    const { queue } = memory(context);
    const worker = new Worker(queue, reportWorker);
    const stop = new AbortController();

    const result = await poll(worker, {
      signal: stop.signal,
      async wait(duration, signal) {
        assert.equal(duration, 100);
        assert.equal(signal, stop.signal);
        stop.abort();
      },
    });

    assert.deepEqual(result, { stopped: "aborted", iterations: 1, jobs: 0, idleIterations: 1 });
  });

  test("stops without waiting when an async batch observer aborts", async (context) => {
    const { queue } = memory(context);
    const worker = new Worker(queue, reportWorker);
    const stop = new AbortController();

    const result = await poll(worker, {
      signal: stop.signal,
      async onBatch() {
        await Promise.resolve();
        stop.abort();
      },
      async wait() { assert.fail("poll already has a stop signal"); },
    });

    assert.deepEqual(result, { stopped: "aborted", iterations: 1, jobs: 0, idleIterations: 1 });
  });

  test("does not claim or notify observers when the signal is already aborted", async (context) => {
    const { queue } = memory(context);
    const { jobId } = queue.enqueue(input());
    const worker = new Worker(queue, reportWorker);

    assert.deepEqual(await poll(worker, {
      signal: AbortSignal.abort(),
      onBatch() { assert.fail("no batch should have run"); },
    }), { stopped: "aborted", iterations: 0, jobs: 0, idleIterations: 0 });
    assert.equal(queue.get(jobId)?.attempts, 0);
  });

  test("propagates observer errors without reverting already completed work", async (context) => {
    const { queue } = memory(context);
    const { jobId } = queue.enqueue(input());
    const worker = new Worker(queue, reportWorker);
    const error = new Error("metric sink unavailable");

    await assert.rejects(poll(worker, {
      async onBatch() { throw error; },
      async wait() { assert.fail("should not wait after an observer error"); },
    }), (cause) => cause === error);
    assert.equal(queue.get(jobId)?.status, "succeeded");
  });
});

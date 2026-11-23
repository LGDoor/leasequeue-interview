import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { describe, test } from "node:test";
import { BufferedProducer } from "../src/index.ts";
import type {
  BufferedProducerOptions,
  EnqueueInput,
  EnqueueResult,
  JsonValue,
  ProducerScheduler,
} from "../src/index.ts";
import { disk, hasCode, input, memory } from "./helpers.ts";

type Timer = {
  dueAt: number;
  run(): void;
  cancelled: boolean;
};

class ManualScheduler {
  private now = 0;
  private readonly timers: Timer[] = [];
  readonly delays: number[] = [];
  cancellations = 0;

  readonly schedule: ProducerScheduler = (delay, run) => {
    const timer = { dueAt: this.now + delay, run, cancelled: false };
    this.timers.push(timer);
    this.delays.push(delay);
    return () => {
      timer.cancelled = true;
      this.cancellations += 1;
    };
  };

  get live(): number {
    return this.timers.filter((timer) => !timer.cancelled).length;
  }

  advance(duration: number): void {
    const target = this.now + duration;
    while (true) {
      const timer = this.timers
        .filter((entry) => !entry.cancelled)
        .sort((left, right) => left.dueAt - right.dueAt)[0];
      if (!timer || timer.dueAt > target) break;
      this.timers.splice(this.timers.indexOf(timer), 1);
      this.now = timer.dueAt;
      timer.run();
    }
    this.now = target;
  }

  fireCancelled(): void {
    for (const timer of this.timers.filter((entry) => entry.cancelled)) {
      timer.run();
    }
  }
}

function mockWriter(write?: (inputs: readonly EnqueueInput[]) => EnqueueResult[]) {
  const batches: EnqueueInput[][] = [];
  let nextId = 0;
  return {
    batches,
    enqueueMany(inputs: readonly EnqueueInput[]): EnqueueResult[] {
      batches.push([...inputs]);
      return write?.(inputs) ?? inputs.map(() => ({ jobId: `job-${++nextId}`, inserted: true }));
    },
  };
}

function setup(options: BufferedProducerOptions = {}) {
  const scheduler = new ManualScheduler();
  const writer = mockWriter();
  const producer = new BufferedProducer(writer, {
    batchSize: 3,
    lingerMs: 15,
    schedule: scheduler.schedule,
    ...options,
  });
  return { producer, writer, scheduler };
}

function busyError(errcode = 5): Error {
  return Object.assign(new Error("database is locked"), { code: "ERR_SQLITE_ERROR", errcode });
}

describe("buffer admission and timing", () => {
  test("is idle at construction and does not open a timer for an empty flush", async () => {
    const { producer, writer, scheduler } = setup();
    assert.deepEqual(producer.getStats(), {
      state: "open", queued: 0, inFlight: 0, accepted: 0,
      inserted: 0, reused: 0, failed: 0, dispatchedBatches: 0, retries: 0,
    });
    assert.deepEqual(await producer.flush(), { accepted: 0, inserted: 0, reused: 0, failed: 0 });
    assert.deepEqual(scheduler.delays, []);
    assert.deepEqual(writer.batches, []);
  });

  test("lingers only until the window measured from the first submission", async () => {
    const { producer, writer, scheduler } = setup();
    const first = producer.enqueue(input({ payload: "first" }));
    scheduler.advance(10);
    const second = producer.enqueue(input({ payload: "second" }));

    scheduler.advance(4);
    assert.equal(writer.batches.length, 0);
    assert.equal(producer.getStats().queued, 2);
    scheduler.advance(1);

    assert.deepEqual(scheduler.delays, [15]);
    assert.deepEqual(writer.batches[0].map(({ payload }) => payload), ["first", "second"]);
    assert.deepEqual(await Promise.all([first, second]), [
      { jobId: "job-1", inserted: true }, { jobId: "job-2", inserted: true },
    ]);
    assert.equal(scheduler.live, 0);
  });

  test("dispatches a full batch promptly and starts a separate linger for the tail", async () => {
    const { producer, writer, scheduler } = setup();
    const results = ["first", "second", "third", "tail"].map((payload) => producer.enqueue(input({ payload })));

    assert.equal(writer.batches.length, 0);
    assert.equal(scheduler.live, 1);
    scheduler.advance(0);
    assert.deepEqual(writer.batches.map((batch) => batch.map(({ payload }) => payload)), [["first", "second", "third"]]);
    assert.equal(producer.getStats().queued, 1);
    assert.deepEqual(scheduler.delays, [15, 0, 15]);
    assert.equal(scheduler.cancellations, 1);

    scheduler.fireCancelled();
    assert.equal(writer.batches.length, 1);
    scheduler.advance(15);
    assert.deepEqual(writer.batches[1].map(({ payload }) => payload), ["tail"]);
    assert.equal((await Promise.all(results)).length, 4);
    assert.equal(producer.getStats().dispatchedBatches, 2);
  });

  test("copies producer input before the caller can mutate it", async () => {
    const { producer, writer, scheduler } = setup();
    const original = input({
      queue: "reports",
      payload: { rows: ["first", "second"] },
      idempotencyKey: "report:today",
      availableAt: 250,
      retryPolicy: { initialDelayMs: 20 },
    });
    const payload = original.payload as { rows: string[] };
    const result = producer.enqueue(original);

    original.queue = "renamed";
    original.availableAt = 999;
    original.retryPolicy!.initialDelayMs = 99;
    payload.rows.reverse();
    scheduler.advance(15);
    await result;

    assert.deepEqual(writer.batches[0][0], {
      queue: "reports", kind: "export", payload: { rows: ["first", "second"] },
      idempotencyKey: "report:today", availableAt: 250, priority: 0, maxAttempts: 3,
      retryPolicy: { initialDelayMs: 20, maxDelayMs: 60_000, multiplier: 2 },
    });
  });

  test("rejects malformed jobs individually without spending queue capacity", async () => {
    const { producer, writer, scheduler } = setup({ batchSize: 2, maxPending: 2 });
    const first = producer.enqueue(input());
    const nonJson = new Date(0) as unknown as JsonValue;

    await assert.rejects(producer.enqueue(input({ payload: nonJson })), hasCode("invalid-input", /payload/));
    await assert.rejects(producer.enqueue(input({ maxAttempts: 0 })), hasCode("invalid-input", /maxAttempts/));
    await assert.rejects(producer.enqueue(input({ queue: "bad queue" })), hasCode("invalid-input", /queue/));
    assert.equal(producer.getStats().accepted, 1);

    const second = producer.enqueue(input());
    scheduler.advance(0);
    await Promise.all([first, second]);
    assert.equal(writer.batches.length, 1);
    assert.equal(writer.batches[0].length, 2);
    assert.equal(producer.getStats().failed, 0);
  });

  test("rejects excess submissions and accepts new work after space becomes available", async () => {
    const { producer, writer, scheduler } = setup({ batchSize: 2, maxPending: 2 });
    const first = producer.enqueue(input({ payload: "first" }));
    const second = producer.enqueue(input({ payload: "second" }));
    await assert.rejects(producer.enqueue(input({ payload: "overflow" })), hasCode("producer-full"));

    scheduler.advance(0);
    await Promise.all([first, second]);
    const third = producer.enqueue(input({ payload: "third" }));
    scheduler.advance(15);
    await third;

    assert.deepEqual(writer.batches.flat().map(({ payload }) => payload), ["first", "second", "third"]);
    assert.equal(producer.getStats().accepted, 3);
  });

  const invalidOptions: { options: BufferedProducerOptions; field: RegExp }[] = [
    { options: { batchSize: 0 }, field: /batch size/ },
    { options: { batchSize: 201 }, field: /batch size/ },
    { options: { batchSize: 10, maxPending: 9 }, field: /pending limit/ },
    { options: { maxPending: 10_001 }, field: /pending limit/ },
    { options: { lingerMs: -1 }, field: /linger/ },
    { options: { lingerMs: 1.5 }, field: /linger/ },
    { options: { retry: { maxRetries: 11 } }, field: /retry count/ },
    { options: { retry: { initialDelayMs: 20, maxDelayMs: 19 } }, field: /maximum retry delay/ },
  ];

  for (const { options, field } of invalidOptions) {
    test(`validates producer options: ${JSON.stringify(options)}`, () => {
      assert.throws(() => new BufferedProducer(mockWriter(), options), hasCode("invalid-input", field));
    });
  }
});

describe("SQLite persistence and results", () => {
  test("resolves requests in their submission order with durable IDs and replay flags", async (context) => {
    const { open } = disk(context);
    const queue = open();
    const reader = open();
    const scheduler = new ManualScheduler();
    const producer = new BufferedProducer(queue, { batchSize: 4, schedule: scheduler.schedule });
    const first = producer.enqueue(input({ payload: "original", idempotencyKey: "weekly" }));
    const second = producer.enqueue(input({ payload: "independent" }));
    const replay = producer.enqueue(input({ payload: "ignored", idempotencyKey: "weekly" }));
    const last = producer.enqueue(input({ queue: "delivery", payload: "dispatch" }));

    assert.equal(reader.status("reports").ready, 0);
    scheduler.advance(0);
    const [original, independent, duplicate, delivery] = await Promise.all([first, second, replay, last]);

    assert.deepEqual(duplicate, { jobId: original.jobId, inserted: false });
    assert.equal(reader.get(original.jobId)?.payload, "original");
    assert.equal(reader.get(independent.jobId)?.payload, "independent");
    assert.equal(reader.get(delivery.jobId)?.queue, "delivery");
    assert.deepEqual(reader.readEvents().map(({ jobId }) => jobId), [original.jobId, independent.jobId, delivery.jobId]);
    assert.deepEqual(producer.getStats(), {
      state: "open", queued: 0, inFlight: 0, accepted: 4,
      inserted: 3, reused: 1, failed: 0, dispatchedBatches: 1, retries: 0,
    });
  });

  test("uses the commit time for availability defaults and preserves explicit timestamps", async (context) => {
    const { queue, clock } = memory(context, 500);
    const scheduler = new ManualScheduler();
    const producer = new BufferedProducer(queue, { batchSize: 2, schedule: scheduler.schedule });
    const given = producer.enqueue(input({ availableAt: 800 }));
    const defaulted = producer.enqueue(input());
    clock.advance(200);
    scheduler.advance(0);
    const [scheduled, ready] = await Promise.all([given, defaulted]);

    assert.equal(queue.get(scheduled.jobId)?.availableAt, 800);
    assert.equal(queue.get(ready.jobId)?.availableAt, 700);
    assert.equal(queue.get(ready.jobId)?.createdAt, 700);
    assert.equal(queue.status("reports").ready, 1);
    assert.equal(queue.status("reports").delayed, 1);
  });

  test("does not own or close the underlying client", async (context) => {
    const { queue } = memory(context);
    const producer = new BufferedProducer(queue);
    const result = producer.enqueue(input());

    assert.deepEqual(await producer.close(), { accepted: 1, inserted: 1, reused: 0, failed: 0 });
    assert.equal(queue.get((await result).jobId)?.status, "queued");
    assert.equal(queue.isClosed, false);
    assert.equal(queue.enqueue(input()).inserted, true);
  });
});

describe("flush and close", () => {
  test("expedites a short batch without waiting for the original linger", async () => {
    const { producer, writer, scheduler } = setup({ lingerMs: 2_000 });
    const submission = producer.enqueue(input());
    const flushed = producer.flush();

    assert.deepEqual(scheduler.delays, [2_000, 0]);
    assert.equal(writer.batches.length, 0);
    scheduler.advance(0);
    assert.deepEqual(await flushed, { accepted: 1, inserted: 1, reused: 0, failed: 0 });
    assert.deepEqual(await submission, { jobId: "job-1", inserted: true });
    scheduler.advance(2_000);
    assert.equal(writer.batches.length, 1);
  });

  test("waits across batch boundaries but leaves a later tail on its own window", async () => {
    const { producer, writer, scheduler } = setup({ batchSize: 2 });
    const accepted = ["one", "two", "three", "four"].map((payload) => producer.enqueue(input({ payload })));
    const flushed = producer.flush();
    const later = producer.enqueue(input({ payload: "later" }));

    scheduler.advance(0);
    assert.deepEqual(await flushed, { accepted: 4, inserted: 4, reused: 0, failed: 0 });
    assert.equal((await Promise.all(accepted)).length, 4);
    assert.equal(producer.getStats().queued, 1);
    assert.equal(writer.batches.length, 2);

    scheduler.advance(15);
    assert.equal((await later).jobId, "job-5");
    assert.equal(writer.batches[2][0].payload, "later");
  });

  test("reports the results for each overlapping flush barrier", async () => {
    const { producer, scheduler } = setup();
    const first = producer.enqueue(input());
    const early = producer.flush();
    const second = producer.enqueue(input());
    const late = producer.flush();

    scheduler.advance(0);
    assert.deepEqual(await early, { accepted: 1, inserted: 1, reused: 0, failed: 0 });
    assert.deepEqual(await late, { accepted: 2, inserted: 2, reused: 0, failed: 0 });
    await Promise.all([first, second]);
    assert.deepEqual(await producer.flush(), { accepted: 0, inserted: 0, reused: 0, failed: 0 });
  });

  test("seals admission immediately and shares the same close result across callers", async () => {
    const { producer, scheduler } = setup();
    const submissions = [producer.enqueue(input()), producer.enqueue(input())];
    const closing = producer.close();
    assert.equal(producer.getStats().state, "closing");
    assert.equal(producer.close(), closing);
    await assert.rejects(producer.enqueue(input()), hasCode("closed"));

    scheduler.advance(0);
    assert.deepEqual(await closing, { accepted: 2, inserted: 2, reused: 0, failed: 0 });
    await Promise.all(submissions);
    assert.equal(producer.getStats().state, "closed");
    assert.equal(producer.close(), closing);
    await assert.rejects(producer.enqueue(input()), hasCode("closed"));
    assert.equal(scheduler.live, 0);
  });

  test("summarizes a reused submission separately from a new insert", async (context) => {
    const { queue } = memory(context);
    const existing = queue.enqueue(input({ idempotencyKey: "published" }));
    const scheduler = new ManualScheduler();
    const producer = new BufferedProducer(queue, { schedule: scheduler.schedule });
    const duplicate = producer.enqueue(input({ idempotencyKey: "published" }));
    const fresh = producer.enqueue(input());
    const flushed = producer.flush();

    scheduler.advance(0);
    assert.deepEqual(await flushed, { accepted: 2, inserted: 1, reused: 1, failed: 0 });
    assert.deepEqual(await duplicate, { jobId: existing.jobId, inserted: false });
    assert.equal((await fresh).inserted, true);
  });

  test("summarizes terminal write errors without hiding individual rejected promises", async () => {
    const scheduler = new ManualScheduler();
    const error = new Error("writer unavailable");
    const writer = mockWriter(() => { throw error; });
    const producer = new BufferedProducer(writer, { schedule: scheduler.schedule });
    const submit = assert.rejects(producer.enqueue(input()), (caught) => caught === error);
    const closing = producer.close();

    scheduler.advance(0);
    await submit;
    assert.deepEqual(await closing, { accepted: 1, inserted: 0, reused: 0, failed: 1 });
    assert.equal(producer.getStats().failed, 1);
    assert.equal(producer.getStats().state, "closed");
    assert.equal(scheduler.live, 0);
  });
});

describe("retrying transactions", () => {
  test("retries an actual busy database and preserves the order of later jobs", async (context) => {
    const { filename, open } = disk(context);
    const queue = open({ busyTimeoutMs: 0 });
    const lock = new DatabaseSync(filename);
    const scheduler = new ManualScheduler();
    const producer = new BufferedProducer(queue, {
      batchSize: 2, lingerMs: 6, schedule: scheduler.schedule,
      retry: { maxRetries: 2, initialDelayMs: 9 },
    });

    const first = producer.enqueue(input({ payload: "first" }));
    const second = producer.enqueue(input({ payload: "second" }));
    try {
      lock.exec("BEGIN IMMEDIATE");
      scheduler.advance(0);
      assert.equal(producer.getStats().inFlight, 2);
      assert.equal(producer.getStats().retries, 1);
      assert.deepEqual(queue.readEvents(), []);
    } finally {
      lock.close();
    }

    const third = producer.enqueue(input({ payload: "third" }));
    scheduler.advance(8);
    assert.equal(queue.status("reports").ready, 0);
    scheduler.advance(1);
    await Promise.all([first, second]);
    assert.equal(queue.status("reports").ready, 2);
    assert.equal(producer.getStats().queued, 1);

    scheduler.advance(6);
    await third;
    assert.deepEqual(queue.list({ queue: "reports" }).jobs.map(({ payload }) => payload), ["first", "second", "third"]);
    assert.equal(producer.getStats().dispatchedBatches, 2);
    assert.equal(producer.getStats().retries, 1);
  });

  test("counts a retrying batch against capacity until that batch settles", async () => {
    const scheduler = new ManualScheduler();
    let attempts = 0;
    const writer = mockWriter((jobs) => {
      attempts += 1;
      if (attempts === 1) throw busyError();
      return jobs.map((job, index) => ({ jobId: `${job.kind}-${index}`, inserted: true }));
    });
    const producer = new BufferedProducer(writer, { batchSize: 2, maxPending: 3, schedule: scheduler.schedule });
    const firstBatch = [producer.enqueue(input()), producer.enqueue(input())];
    scheduler.advance(0);
    const next = producer.enqueue(input());

    await assert.rejects(producer.enqueue(input()), hasCode("producer-full"));
    assert.equal(producer.getStats().inFlight, 2);
    assert.equal(producer.getStats().queued, 1);
    assert.equal(writer.batches.length, 1);

    scheduler.advance(10);
    await Promise.all(firstBatch);
    const last = producer.enqueue(input());
    scheduler.advance(0);
    await Promise.all([next, last]);
    assert.equal(producer.getStats().inserted, 4);
  });

  test("caps exponential delays and rejects with the final busy error after exhausting retries", async () => {
    const scheduler = new ManualScheduler();
    const extendedBusy = busyError(5 | (2 << 8));
    const writer = mockWriter(() => { throw extendedBusy; });
    const producer = new BufferedProducer(writer, {
      batchSize: 1, schedule: scheduler.schedule,
      retry: { maxRetries: 3, initialDelayMs: 4, maxDelayMs: 9 },
    });
    const rejected = assert.rejects(producer.enqueue(input()), (caught) => caught === extendedBusy);

    scheduler.advance(20);
    assert.equal(writer.batches.length, 3);
    assert.equal(producer.getStats().inFlight, 1);
    scheduler.advance(1);
    await rejected;

    assert.deepEqual(scheduler.delays, [0, 4, 8, 9]);
    assert.equal(writer.batches.length, 4);
    assert.equal(producer.getStats().retries, 3);
    assert.equal(producer.getStats().failed, 1);
    assert.equal(producer.getStats().inFlight, 0);
    assert.equal(scheduler.live, 0);
  });

  test("allows applications to disable retries", async () => {
    const scheduler = new ManualScheduler();
    const writer = mockWriter(() => { throw busyError(); });
    const producer = new BufferedProducer(writer, {
      batchSize: 1, schedule: scheduler.schedule, retry: { maxRetries: 0 },
    });
    const rejected = assert.rejects(producer.enqueue(input()), /database is locked/);

    scheduler.advance(0);
    await rejected;
    assert.equal(writer.batches.length, 1);
    assert.equal(producer.getStats().retries, 0);
    assert.equal(scheduler.live, 0);
  });

  test("does not retry non-busy failures and continues to dispatch later batches", async () => {
    const scheduler = new ManualScheduler();
    const constraint = busyError(19);
    let attempts = 0;
    const writer = mockWriter(() => {
      attempts += 1;
      if (attempts === 1) throw constraint;
      return [{ jobId: "saved", inserted: true }];
    });
    const producer = new BufferedProducer(writer, { batchSize: 1, schedule: scheduler.schedule });
    const rejected = assert.rejects(producer.enqueue(input()), (caught) => caught === constraint);
    const accepted = producer.enqueue(input());

    scheduler.advance(0);
    await rejected;
    assert.equal((await accepted).jobId, "saved");
    assert.equal(producer.getStats().retries, 0);
    assert.equal(producer.getStats().inserted, 1);
    assert.equal(producer.getStats().failed, 1);
  });

  test("does not resolve a caller with the wrong result when a custom writer breaks its contract", async () => {
    const scheduler = new ManualScheduler();
    const producer = new BufferedProducer(mockWriter(() => []), { batchSize: 2, schedule: scheduler.schedule });
    const rejected = [producer.enqueue(input()), producer.enqueue(input())]
      .map((result) => assert.rejects(result, hasCode("invalid-data", /wrong number of results/)));

    scheduler.advance(0);
    await Promise.all(rejected);
    assert.equal(producer.getStats().failed, 2);
    assert.equal(producer.getStats().retries, 0);
  });
});

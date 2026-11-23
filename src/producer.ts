import type { QueueClient } from "./client.ts";
import { QueueError } from "./errors.ts";
import type { EnqueueInput, EnqueueResult, JsonValue } from "./model.ts";
import { integer, invalid, limits, normalizeEnqueue } from "./validate.ts";

export type ProducerScheduler = (delayMs: number, run: () => void) => () => void;

export type ProducerRetryOptions = {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
};

export type BufferedProducerOptions = {
  batchSize?: number;
  maxPending?: number;
  lingerMs?: number;
  retry?: ProducerRetryOptions;
  schedule?: ProducerScheduler;
};

export type ProducerState = "open" | "closing" | "closed";

export type ProducerStats = {
  state: ProducerState;
  queued: number;
  inFlight: number;
  accepted: number;
  inserted: number;
  reused: number;
  failed: number;
  dispatchedBatches: number;
  retries: number;
};

export type ProducerFlushResult = {
  accepted: number;
  inserted: number;
  reused: number;
  failed: number;
};

type PendingEntry = {
  sequence: number;
  input: EnqueueInput;
  promise: Promise<EnqueueResult>;
  resolve(result: EnqueueResult): void;
  reject(reason: unknown): void;
};

type Batch = {
  entries: PendingEntry[];
  inputs: EnqueueInput[];
  retries: number;
};

type Wakeup = {
  kind: "linger" | "immediate" | "retry";
  cancel(): void;
};

const sqliteBusyCode = 5;
const sqlitePrimaryCodeMask = 0xff;

function scheduleTimer(delayMs: number, run: () => void): () => void {
  const timeout = setTimeout(run, delayMs);
  return () => clearTimeout(timeout);
}

function isBusy(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const sqlite = error as { code?: unknown; errcode?: unknown };
  return sqlite.code === "ERR_SQLITE_ERROR"
    && typeof sqlite.errcode === "number"
    && (sqlite.errcode & sqlitePrimaryCodeMask) === sqliteBusyCode;
}

function snapshot(input: EnqueueInput): EnqueueInput {
  if (!input || typeof input !== "object") invalid("enqueue input", "must be an object");
  const submitted = { ...input };
  const normalized = normalizeEnqueue(submitted, 0);

  return {
    queue: normalized.queue,
    kind: normalized.kind,
    payload: JSON.parse(normalized.payloadJson) as JsonValue,
    availableAt: submitted.availableAt,
    idempotencyKey: normalized.idempotencyKey ?? undefined,
    priority: normalized.priority,
    maxAttempts: normalized.maxAttempts,
    retryPolicy: normalized.retryPolicy,
  };
}

function pendingEntry(sequence: number, input: EnqueueInput): PendingEntry {
  let resolve!: PendingEntry["resolve"];
  let reject!: PendingEntry["reject"];
  const promise = new Promise<EnqueueResult>((success, failure) => {
    resolve = success;
    reject = failure;
  });
  return { sequence, input, promise, resolve, reject };
}

function summarize(results: PromiseSettledResult<EnqueueResult>[]): ProducerFlushResult {
  const summary = { accepted: results.length, inserted: 0, reused: 0, failed: 0 };
  for (const result of results) {
    if (result.status === "rejected") summary.failed += 1;
    else if (result.value.inserted) summary.inserted += 1;
    else summary.reused += 1;
  }
  return summary;
}

export class BufferedProducer {
  private readonly writer: Pick<QueueClient, "enqueueMany">;
  private readonly batchSize: number;
  private readonly maxPending: number;
  private readonly lingerMs: number;
  private readonly retry: Required<ProducerRetryOptions>;
  private readonly schedule: ProducerScheduler;
  private readonly pending: PendingEntry[] = [];
  private readonly totals = {
    accepted: 0,
    inserted: 0,
    reused: 0,
    failed: 0,
    dispatchedBatches: 0,
    retries: 0,
  };
  private state: ProducerState = "open";
  private active: Batch | undefined;
  private wakeup: Wakeup | undefined;
  private flushThrough = 0;
  private closing: Promise<ProducerFlushResult> | undefined;

  constructor(writer: Pick<QueueClient, "enqueueMany">, options: BufferedProducerOptions = {}) {
    this.writer = writer;
    this.batchSize = integer(options.batchSize ?? 50, "producer batch size", 1, limits.batch);
    this.maxPending = integer(options.maxPending ?? 1_000, "producer pending limit", this.batchSize, 10_000);
    this.lingerMs = integer(options.lingerMs ?? 10, "producer linger", 0, 60_000);
    const initialDelayMs = integer(options.retry?.initialDelayMs ?? 10, "producer initial retry delay", 0, 60_000);
    this.retry = {
      maxRetries: integer(options.retry?.maxRetries ?? 3, "producer retry count", 0, 10),
      initialDelayMs,
      maxDelayMs: integer(options.retry?.maxDelayMs ?? 250, "producer maximum retry delay", initialDelayMs, 60_000),
    };
    this.schedule = options.schedule ?? scheduleTimer;
    if (typeof this.schedule !== "function") invalid("producer scheduler", "must be a function");
  }

  getStats(): ProducerStats {
    return {
      state: this.state,
      queued: this.pending.length,
      inFlight: this.active?.entries.length ?? 0,
      ...this.totals,
    };
  }

  enqueue(input: EnqueueInput): Promise<EnqueueResult> {
    if (this.state !== "open") {
      return Promise.reject(new QueueError("closed", "buffered producer is not accepting jobs"));
    }
    if (this.size >= this.maxPending) {
      return Promise.reject(new QueueError("producer-full", "buffered producer has reached its pending limit"));
    }

    let saved: EnqueueInput;
    try {
      saved = snapshot(input);
    } catch (error) {
      return Promise.reject(error);
    }

    this.totals.accepted += 1;
    const entry = pendingEntry(this.totals.accepted, saved);
    this.pending.push(entry);
    this.plan();
    return entry.promise;
  }

  flush(): Promise<ProducerFlushResult> {
    const barrier = this.pending.slice();
    const latest = barrier.at(-1);
    if (latest) this.flushThrough = Math.max(this.flushThrough, latest.sequence);
    this.plan();
    return Promise.allSettled(barrier.map((entry) => entry.promise)).then(summarize);
  }

  close(): Promise<ProducerFlushResult> {
    if (this.closing) return this.closing;

    this.state = "closing";
    this.closing = this.flush().then((result) => {
      this.state = "closed";
      return result;
    });
    return this.closing;
  }

  private get size(): number {
    return this.pending.length + (this.active?.entries.length ?? 0);
  }

  private plan(): void {
    if (this.active || this.pending.length === 0) return;
    const immediate = this.pending.length >= this.batchSize
      || this.pending[0].sequence <= this.flushThrough;

    if (immediate && this.wakeup?.kind !== "immediate") {
      this.arm("immediate", 0);
    } else if (!this.wakeup) {
      this.arm("linger", this.lingerMs);
    }
  }

  private arm(kind: Wakeup["kind"], delayMs: number): void {
    this.wakeup?.cancel();
    const wakeup: Wakeup = {
      kind,
      cancel: this.schedule(delayMs, () => {
        if (this.wakeup !== wakeup) return;
        this.wakeup = undefined;
        this.dispatch();
      }),
    };
    this.wakeup = wakeup;
  }

  private dispatch(): void {
    let batch = this.active;
    if (!batch) {
      const entries = this.pending.splice(0, this.batchSize);
      if (entries.length === 0) return;
      batch = { entries, inputs: entries.map((entry) => entry.input), retries: 0 };
      this.active = batch;
      this.totals.dispatchedBatches += 1;
    }

    let results: EnqueueResult[];
    try {
      results = this.writer.enqueueMany(batch.inputs);
      if (results.length !== batch.entries.length) {
        throw new QueueError("invalid-data", "batch writer returned the wrong number of results");
      }
    } catch (error) {
      if (isBusy(error) && batch.retries < this.retry.maxRetries) {
        const delay = Math.min(this.retry.maxDelayMs, this.retry.initialDelayMs * 2 ** batch.retries);
        batch.retries += 1;
        this.totals.retries += 1;
        this.arm("retry", delay);
        return;
      }

      this.totals.failed += batch.entries.length;
      for (const entry of batch.entries) entry.reject(error);
      this.active = undefined;
      this.plan();
      return;
    }

    for (const [index, result] of results.entries()) {
      if (result.inserted) this.totals.inserted += 1;
      else this.totals.reused += 1;
      batch.entries[index].resolve(result);
    }
    this.active = undefined;
    this.plan();
  }
}

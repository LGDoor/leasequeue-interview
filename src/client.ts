import { readTime, systemClock } from "./clock.ts";
import { QueueError } from "./errors.ts";
import type { PruneEventsOptions, QueueEvent, ReadEventsOptions } from "./events.ts";
import type {
  ClaimOptions,
  ClaimedJob,
  Clock,
  EnqueueInput,
  EnqueueResult,
  FailOptions,
  FailResult,
  Job,
  JobPage,
  LeaseRef,
  ListJobsOptions,
  QueueSettings,
  QueueStatus,
  ReclaimOptions,
  ReclaimResult,
  RetryDeadOptions,
} from "./model.ts";
import { SqliteStore } from "./sqlite/store.ts";
import {
  boolean,
  integer,
  invalid,
  leaseRefFields,
  limits,
  name,
  normalizeEnqueue,
  status,
  text,
  timestamp,
} from "./validate.ts";

export type OpenQueueOptions = {
  clock?: Clock;
  busyTimeoutMs?: number;
};

export class QueueClient {
  private readonly store: SqliteStore;
  private readonly clock: Clock;
  private closed = false;

  constructor(filename: string, options: OpenQueueOptions = {}) {
    const path = text(filename, "database path", 4_096);
    const timeout = integer(options.busyTimeoutMs ?? 5_000, "SQLite busy timeout", 0, 60_000);
    this.clock = options.clock ?? systemClock;
    this.store = new SqliteStore(path, timeout);
  }

  get isClosed(): boolean {
    return this.closed;
  }

  close(): void {
    if (!this.closed) {
      this.store.close();
      this.closed = true;
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new QueueError("closed", "queue client is closed");
  }

  private now(): number {
    this.assertOpen();
    return readTime(this.clock);
  }

  enqueue(input: EnqueueInput): EnqueueResult {
    const now = this.now();
    return this.store.enqueue(normalizeEnqueue(input, now), now);
  }

  enqueueMany(inputs: readonly EnqueueInput[]): EnqueueResult[] {
    const now = this.now();
    if (!Array.isArray(inputs) || inputs.length > limits.batch) {
      invalid("enqueue batch", `must contain at most ${limits.batch} jobs`);
    }

    if (inputs.length === 0) return [];
    const normalized = Array.from(inputs, (input) => normalizeEnqueue(input, now));
    return this.store.enqueueMany(normalized, now);
  }

  get(jobId: string): Job | null {
    this.assertOpen();
    return this.store.get(text(jobId, "job ID", 120));
  }

  list(options: ListJobsOptions): JobPage {
    this.assertOpen();
    return this.store.list({
      queue: name(options.queue, "queue"),
      status: options.status === undefined ? undefined : status(options.status),
      after: timestamp(options.after ?? 0, "cursor"),
      limit: integer(options.limit ?? 50, "page limit", 1, limits.page),
    });
  }

  claim(options: ClaimOptions): ClaimedJob | null {
    const now = this.now();
    const normalized = {
      queue: name(options.queue, "queue"),
      workerId: name(options.workerId, "worker ID"),
      leaseMs: integer(options.leaseMs ?? 30_000, "lease duration", 1, limits.leaseMs),
    };

    return this.store.claim(normalized, now);
  }

  renew(lease: LeaseRef, leaseMs: number): ClaimedJob | null {
    const now = this.now();
    const duration = integer(leaseMs, "lease duration", 1, limits.leaseMs);
    return this.store.renew(leaseRefFields(lease), duration, now);
  }

  complete(lease: LeaseRef): boolean {
    const now = this.now();
    return this.store.complete(leaseRefFields(lease), now);
  }

  fail(lease: LeaseRef, failure: FailOptions): FailResult {
    const now = this.now();
    const normalized = {
      message: text(failure.message, "failure message", 2_048),
      permanent: failure.permanent === undefined ? undefined : boolean(failure.permanent, "permanent"),
      retryAfterMs: failure.retryAfterMs === undefined
        ? undefined
        : integer(failure.retryAfterMs, "retry delay", 0, limits.leaseMs),
    };

    return this.store.fail(leaseRefFields(lease), normalized, now);
  }

  cancel(jobId: string): boolean {
    const now = this.now();
    return this.store.cancel(text(jobId, "job ID", 120), now);
  }

  retryDead(jobId: string, options: RetryDeadOptions = {}): Job | null {
    const now = this.now();
    const next = {
      availableAt: timestamp(options.availableAt ?? now, "availableAt"),
      maxAttempts: options.maxAttempts === undefined
        ? undefined
        : integer(options.maxAttempts, "maxAttempts", 1, limits.attempts),
    };

    return this.store.retryDead(text(jobId, "job ID", 120), next, now);
  }

  reclaimExpired(options: ReclaimOptions = {}): ReclaimResult {
    const now = this.now();
    return this.store.reclaimExpired({
      queue: options.queue === undefined ? undefined : name(options.queue, "queue"),
      limit: integer(options.limit ?? 100, "reclaim limit", 1, limits.page),
    }, now);
  }

  getSettings(queue: string): QueueSettings {
    this.assertOpen();
    return this.store.getSettings(name(queue, "queue"));
  }

  setPaused(queue: string, paused: boolean): QueueSettings {
    const now = this.now();
    return this.store.setPaused(name(queue, "queue"), boolean(paused, "paused"), now);
  }

  status(queue: string): QueueStatus {
    const now = this.now();
    return this.store.status(name(queue, "queue"), now);
  }

  readEvents(options: ReadEventsOptions = {}): QueueEvent[] {
    this.assertOpen();
    return this.store.readEvents({
      queue: options.queue === undefined ? undefined : name(options.queue, "queue"),
      jobId: options.jobId === undefined ? undefined : text(options.jobId, "job ID", 120),
      after: timestamp(options.after ?? 0, "event cursor"),
      limit: integer(options.limit ?? 50, "event limit", 1, limits.page),
    });
  }

  pruneEvents(options: PruneEventsOptions): number {
    this.assertOpen();
    return this.store.pruneEvents({
      before: timestamp(options.before, "event cutoff"),
      queue: options.queue === undefined ? undefined : name(options.queue, "queue"),
    });
  }
}

export function openQueue(filename: string, options: OpenQueueOptions = {}): QueueClient {
  return new QueueClient(filename, options);
}

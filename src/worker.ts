import type { QueueClient } from "./client.ts";
import { failureMessage, PermanentJobError, RetryAfterError } from "./errors.ts";
import { leaseRef } from "./model.ts";
import type { ClaimedJob, FailOptions } from "./model.ts";
import { integer, invalid, limits, name } from "./validate.ts";

export type JobContext = {
  job: ClaimedJob;
  renew(leaseMs: number): ClaimedJob | null;
};

export type JobHandler = (context: JobContext) => void | Promise<void>;

export type WorkerOptions = {
  queue: string;
  workerId: string;
  leaseMs?: number;
  handlers: Record<string, JobHandler>;
};

export type WorkerOutcome =
  | { outcome: "idle" }
  | { outcome: "succeeded" | "dead" | "lost"; jobId: string }
  | { outcome: "retry"; jobId: string; availableAt: number };

export type JobOutcome = Exclude<WorkerOutcome, { outcome: "idle" }>;

export type BatchOptions = {
  limit?: number;
  signal?: AbortSignal;
};

export type BatchResult = {
  results: JobOutcome[];
  drained: boolean;
};

function classify(error: unknown): FailOptions {
  return {
    message: failureMessage(error),
    permanent: error instanceof PermanentJobError,
    retryAfterMs: error instanceof RetryAfterError ? error.delayMs : undefined,
  };
}

export class Worker {
  private readonly client: QueueClient;
  private readonly queue: string;
  private readonly workerId: string;
  private readonly leaseMs: number;
  private readonly handlers = new Map<string, JobHandler>();

  constructor(client: QueueClient, options: WorkerOptions) {
    this.client = client;
    this.queue = name(options.queue, "queue");
    this.workerId = name(options.workerId, "worker ID");
    this.leaseMs = integer(options.leaseMs ?? 30_000, "lease duration", 1, limits.leaseMs);

    for (const [kind, handler] of Object.entries(options.handlers)) {
      const key = name(kind, "job kind");
      if (typeof handler !== "function") invalid("handler", "must be a function");
      this.handlers.set(key, handler);
    }
  }

  async runOnce(): Promise<WorkerOutcome> {
    const job = this.client.claim({
      queue: this.queue,
      workerId: this.workerId,
      leaseMs: this.leaseMs,
    });
    if (!job) return { outcome: "idle" };

    const lease = leaseRef(job);
    const handler = this.handlers.get(job.kind);
    let failure: FailOptions | undefined;

    if (!handler) {
      failure = { message: `No handler for ${job.kind}`, permanent: true };
    } else {
      try {
        await handler({ job, renew: (leaseMs) => this.client.renew(lease, leaseMs) });
      } catch (error) {
        failure = classify(error);
      }
    }

    if (failure) {
      return { ...this.client.fail(lease, failure), jobId: job.id };
    }

    const completed = this.client.complete(lease);
    return { outcome: completed ? "succeeded" : "lost", jobId: job.id };
  }

  async runBatch(options: BatchOptions = {}): Promise<BatchResult> {
    const limit = integer(options.limit ?? 10, "batch limit", 1, limits.batch);
    const results: JobOutcome[] = [];

    while (results.length < limit && !options.signal?.aborted) {
      const result = await this.runOnce();

      if (result.outcome === "idle") {
        return { results, drained: true };
      }

      results.push(result);
    }

    return { results, drained: false };
  }
}

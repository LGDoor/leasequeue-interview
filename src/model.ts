export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;

export type JsonObject = { [key: string]: JsonValue };

export const jobStatuses = [
  "queued",
  "running",
  "succeeded",
  "dead",
  "cancelled",
] as const;

export type JobStatus = (typeof jobStatuses)[number];

export type RetryPolicy = {
  initialDelayMs: number;
  maxDelayMs: number;
  multiplier: number;
};

export const defaultRetryPolicy: Readonly<RetryPolicy> = Object.freeze({
  initialDelayMs: 1_000,
  maxDelayMs: 60_000,
  multiplier: 2,
});

export type Clock = {
  now(): number;
};

export type EnqueueInput = {
  queue: string;
  kind: string;
  payload: JsonValue;
  idempotencyKey?: string;
  priority?: number;
  availableAt?: number;
  maxAttempts?: number;
  retryPolicy?: Partial<RetryPolicy>;
};

export type EnqueueResult = {
  jobId: string;
  inserted: boolean;
};

export type JobLease = {
  token: string;
  workerId: string;
  expiresAt: number;
};

export type Job = {
  id: string;
  queue: string;
  kind: string;
  payload: JsonValue;
  status: JobStatus;
  priority: number;
  availableAt: number;
  attempts: number;
  maxAttempts: number;
  retryPolicy: RetryPolicy;
  idempotencyKey: string | null;
  lease: JobLease | null;
  lastError: string | null;
  createdAt: number;
  updatedAt: number;
  finishedAt: number | null;
};

export type ClaimedJob = Job & {
  status: "running";
  lease: JobLease;
};

export type LeaseRef = {
  jobId: string;
  token: string;
};

export type ClaimOptions = {
  queue: string;
  workerId: string;
  leaseMs?: number;
};

export type FailOptions = {
  message: string;
  permanent?: boolean;
  retryAfterMs?: number;
};

export type FailResult =
  | { outcome: "retry"; availableAt: number }
  | { outcome: "dead" }
  | { outcome: "lost" };

export type RetryDeadOptions = {
  availableAt?: number;
  maxAttempts?: number;
};

export type ListJobsOptions = {
  queue: string;
  status?: JobStatus;
  after?: number;
  limit?: number;
};

export type JobPage = {
  jobs: Job[];
  nextCursor: number | null;
};

export type QueueStatus = {
  queue: string;
  paused: boolean;
  counts: Record<JobStatus, number>;
  ready: number;
  delayed: number;
  expired: number;
  nextAvailableAt: number | null;
};

export type ReclaimOptions = {
  queue?: string;
  limit?: number;
};

export type ReclaimResult = {
  requeued: string[];
  dead: string[];
};

export type QueueSettings = {
  queue: string;
  paused: boolean;
  updatedAt: number | null;
};

export function leaseRef(job: ClaimedJob): LeaseRef {
  return { jobId: job.id, token: job.lease.token };
}

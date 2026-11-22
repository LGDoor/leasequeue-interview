import { timeAfter } from "./clock.ts";
import type { ClaimedJob, FailOptions, RetryPolicy } from "./model.ts";

export type FailurePlan = {
  state: "queued";
  availableAt: number;
} | {
  state: "dead";
};

export function backoffDelay(policy: RetryPolicy, attempts: number): number {
  if (policy.initialDelayMs === 0) return 0;

  const exponent = Math.max(0, attempts - 1);
  const scaled = policy.initialDelayMs * policy.multiplier ** exponent;
  return Math.min(policy.maxDelayMs, Math.ceil(scaled));
}

export function planFailure(job: ClaimedJob, failure: FailOptions, now: number): FailurePlan {
  if (failure.permanent || job.attempts >= job.maxAttempts) {
    return { state: "dead" };
  }

  const delay = failure.retryAfterMs ?? backoffDelay(job.retryPolicy, job.attempts);
  return { state: "queued", availableAt: timeAfter(now, delay) };
}

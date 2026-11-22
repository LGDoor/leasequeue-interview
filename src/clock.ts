import { QueueError } from "./errors.ts";
import type { Clock } from "./model.ts";

export const systemClock: Clock = Object.freeze({ now: () => Date.now() });

export function readTime(clock: Clock): number {
  const now = clock.now();

  if (!Number.isSafeInteger(now) || now < 0) {
    throw new QueueError("invalid-input", "clock must return a nonnegative integer timestamp");
  }

  return now;
}

export function timeAfter(now: number, delayMs: number): number {
  const next = now + delayMs;

  if (!Number.isSafeInteger(next) || delayMs < 0) {
    throw new QueueError("invalid-input", "time plus delay is outside the supported range");
  }

  return next;
}

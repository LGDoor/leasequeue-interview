import { QueueError } from "./errors.ts";
import { defaultRetryPolicy, jobStatuses } from "./model.ts";
import type {
  EnqueueInput,
  JobStatus,
  JsonValue,
  LeaseRef,
  RetryPolicy,
} from "./model.ts";

export const limits = {
  leaseMs: 86_400_000,
  priority: 10_000,
  attempts: 100,
  batch: 200,
  page: 250,
  payloadBytes: 64_000,
  payloadDepth: 40,
} as const;

export type NormalizedEnqueue = {
  queue: string;
  kind: string;
  payloadJson: string;
  idempotencyKey: string | null;
  priority: number;
  availableAt: number;
  maxAttempts: number;
  retryPolicy: RetryPolicy;
};

export function invalid(field: string, expectation: string): never {
  throw new QueueError("invalid-input", `${field} ${expectation}`);
}

export function integer(value: unknown, field: string, minimum: number, maximum: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
    invalid(field, `must be an integer between ${minimum} and ${maximum}`);
  }

  return value;
}

export function timestamp(value: unknown, field: string): number {
  return integer(value, field, 0, Number.MAX_SAFE_INTEGER);
}

export function text(value: unknown, field: string, maximum: number): string {
  if (typeof value !== "string" || !value.trim() || value.length > maximum || value.includes("\0")) {
    invalid(field, `must be a nonblank string of at most ${maximum} characters without NUL`);
  }

  return value;
}

export function name(value: unknown, field: string): string {
  const result = text(value, field, 100);

  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.test(result)) {
    invalid(field, "may contain only letters, digits, dots, underscores, and hyphens");
  }

  return result;
}

export function boolean(value: unknown, field: string): boolean {
  if (typeof value !== "boolean") invalid(field, "must be a boolean");
  return value;
}

export function status(value: unknown): JobStatus {
  const result = jobStatuses.find((entry) => entry === value);
  if (!result) invalid("status", "must be a supported job status");
  return result;
}

export function leaseRefFields(lease: LeaseRef): LeaseRef {
  return {
    jobId: text(lease.jobId, "job ID", 120),
    token: text(lease.token, "lease token", 120),
  };
}

export function retryPolicy(value: Partial<RetryPolicy> = {}, base = defaultRetryPolicy): RetryPolicy {
  const initialDelayMs = integer(
    value.initialDelayMs ?? base.initialDelayMs,
    "initial retry delay",
    0,
    limits.leaseMs,
  );

  const maxDelayMs = integer(
    value.maxDelayMs ?? base.maxDelayMs,
    "maximum retry delay",
    initialDelayMs,
    limits.leaseMs,
  );

  const multiplier = value.multiplier ?? base.multiplier;
  if (!Number.isFinite(multiplier) || multiplier < 1 || multiplier > 10) {
    invalid("retry multiplier", "must be between 1 and 10");
  }

  return { initialDelayMs, maxDelayMs, multiplier };
}

function assertJson(value: unknown, depth: number, stack: Set<object>): asserts value is JsonValue {
  if (depth > limits.payloadDepth) invalid("payload", "is too deeply nested");
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (typeof value !== "object" || value === null) invalid("payload", "must contain only JSON values");
  if (stack.has(value)) invalid("payload", "must not contain circular references");

  stack.add(value);

  if (Array.isArray(value)) {
    for (const entry of value) {
      assertJson(entry, depth + 1, stack);
    }
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      invalid("payload", "must contain only plain objects and JSON values");
    }

    for (const entry of Object.values(value)) {
      assertJson(entry, depth + 1, stack);
    }
  }

  stack.delete(value);
}

export function encodePayload(value: unknown): string {
  assertJson(value, 0, new Set());
  const json = JSON.stringify(value);

  if (Buffer.byteLength(json) > limits.payloadBytes) {
    invalid("payload", `must encode to at most ${limits.payloadBytes} bytes`);
  }

  return json;
}

export function normalizeEnqueue(input: EnqueueInput, now: number): NormalizedEnqueue {
  return {
    queue: name(input.queue, "queue"),
    kind: name(input.kind, "job kind"),
    payloadJson: encodePayload(input.payload),
    idempotencyKey: input.idempotencyKey === undefined
      ? null
      : text(input.idempotencyKey, "idempotency key", 200),
    priority: integer(input.priority ?? 0, "priority", -limits.priority, limits.priority),
    availableAt: timestamp(input.availableAt ?? now, "availableAt"),
    maxAttempts: integer(input.maxAttempts ?? 3, "maxAttempts", 1, limits.attempts),
    retryPolicy: retryPolicy(input.retryPolicy),
  };
}

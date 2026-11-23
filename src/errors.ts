export type QueueErrorCode =
  | "invalid-input"
  | "closed"
  | "producer-full"
  | "invalid-schema"
  | "invalid-data";

export class QueueError extends Error {
  readonly code: QueueErrorCode;

  constructor(code: QueueErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "QueueError";
    this.code = code;
  }
}

export class PermanentJobError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "PermanentJobError";
  }
}

export class RetryAfterError extends Error {
  readonly delayMs: number;

  constructor(message: string, delayMs: number, options?: ErrorOptions) {
    super(message, options);
    if (!Number.isSafeInteger(delayMs) || delayMs < 0) {
      throw new QueueError("invalid-input", "retry delay must be a nonnegative integer");
    }

    this.name = "RetryAfterError";
    this.delayMs = delayMs;
  }
}

export function failureMessage(error: unknown): string {
  const message = error instanceof Error
    ? error.message
    : typeof error === "string"
      ? error
      : "Job handler failed";

  return (message.trim() || "Job handler failed").slice(0, 2_048);
}

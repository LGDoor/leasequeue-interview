import type { SQLOutputValue } from "node:sqlite";
import { QueueError } from "../errors.ts";
import { eventNames } from "../events.ts";
import type { QueueEvent } from "../events.ts";
import { jobStatuses } from "../model.ts";
import type { Job, JsonObject, JsonValue, QueueSettings } from "../model.ts";

export type Row = Record<string, SQLOutputValue>;

export function numberColumn(row: Row, column: string): number {
  const value = row[column];
  if (typeof value !== "number" || !Number.isFinite(value)) badColumn(column);
  return value;
}

export function textColumn(row: Row, column: string): string {
  const value = row[column];
  if (typeof value !== "string") badColumn(column);
  return value;
}

function optionalNumber(row: Row, column: string): number | null {
  return row[column] === null ? null : numberColumn(row, column);
}

function optionalText(row: Row, column: string): string | null {
  return row[column] === null ? null : textColumn(row, column);
}

function badColumn(column: string): never {
  throw new QueueError("invalid-data", `invalid SQLite column: ${column}`);
}

function jsonColumn(row: Row, column: string): JsonValue {
  try {
    return JSON.parse(textColumn(row, column)) as JsonValue;
  } catch (error) {
    throw new QueueError("invalid-data", `invalid JSON in SQLite column: ${column}`, { cause: error });
  }
}

export function toJob(row: Row): Job {
  const state = jobStatuses.find((entry) => entry === row.state);
  if (!state) badColumn("state");

  return {
    id: textColumn(row, "id"),
    queue: textColumn(row, "queue"),
    kind: textColumn(row, "kind"),
    payload: jsonColumn(row, "payload_json"),
    status: state,
    priority: numberColumn(row, "priority"),
    availableAt: numberColumn(row, "available_at"),
    attempts: numberColumn(row, "attempts"),
    maxAttempts: numberColumn(row, "max_attempts"),
    retryPolicy: {
      initialDelayMs: numberColumn(row, "retry_initial_ms"),
      maxDelayMs: numberColumn(row, "retry_max_ms"),
      multiplier: numberColumn(row, "retry_multiplier"),
    },
    idempotencyKey: optionalText(row, "idempotency_key"),
    lease: state === "running" ? {
      token: textColumn(row, "lease_token"),
      workerId: textColumn(row, "worker_id"),
      expiresAt: numberColumn(row, "lease_until"),
    } : null,
    lastError: optionalText(row, "last_error"),
    createdAt: numberColumn(row, "created_at"),
    updatedAt: numberColumn(row, "updated_at"),
    finishedAt: optionalNumber(row, "finished_at"),
  };
}

export function toEvent(row: Row): QueueEvent {
  const name = eventNames.find((entry) => entry === row.event_name);
  if (!name) badColumn("event_name");

  const details = jsonColumn(row, "details_json");
  if (!details || typeof details !== "object" || Array.isArray(details)) {
    badColumn("details_json");
  }

  return {
    sequence: numberColumn(row, "sequence"),
    queue: textColumn(row, "queue"),
    jobId: optionalText(row, "job_id"),
    occurredAt: numberColumn(row, "occurred_at"),
    name,
    details: details as JsonObject,
  };
}

export function toSettings(queue: string, row: Row | undefined): QueueSettings {
  return row ? {
    queue,
    paused: numberColumn(row, "paused") === 1,
    updatedAt: numberColumn(row, "updated_at"),
  } : { queue, paused: false, updatedAt: null };
}

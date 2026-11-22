import type { JsonObject } from "./model.ts";

export const eventNames = [
  "enqueued",
  "claimed",
  "lease-renewed",
  "succeeded",
  "retry-scheduled",
  "dead",
  "lease-expired",
  "cancelled",
  "retried",
  "queue-paused",
  "queue-resumed",
] as const;

export type EventName = (typeof eventNames)[number];

export type QueueEvent = {
  sequence: number;
  queue: string;
  jobId: string | null;
  name: EventName;
  occurredAt: number;
  details: JsonObject;
};

export type ReadEventsOptions = {
  queue?: string;
  jobId?: string;
  after?: number;
  limit?: number;
};

export type PruneEventsOptions = {
  before: number;
  queue?: string;
};

import { randomUUID } from "node:crypto";
import type { DatabaseSync, SQLInputValue } from "node:sqlite";
import { timeAfter } from "../clock.ts";
import { QueueError } from "../errors.ts";
import type { EventName, PruneEventsOptions, QueueEvent, ReadEventsOptions } from "../events.ts";
import { jobStatuses } from "../model.ts";
import type {
  ClaimOptions,
  ClaimedJob,
  EnqueueResult,
  FailOptions,
  FailResult,
  Job,
  JobPage,
  JsonObject,
  LeaseRef,
  ListJobsOptions,
  QueueSettings,
  QueueStatus,
  ReclaimOptions,
  ReclaimResult,
  RetryDeadOptions,
} from "../model.ts";
import { planFailure } from "../retry.ts";
import type { NormalizedEnqueue } from "../validate.ts";
import { openDatabase, writeTransaction } from "./database.ts";
import { numberColumn, textColumn, toEvent, toJob, toSettings } from "./codec.ts";
import type { Row } from "./codec.ts";

function requiredRow(row: Row | undefined): Row {
  if (!row) throw new QueueError("invalid-data", "expected SQLite row was not returned");
  return row;
}

function asClaim(job: Job): ClaimedJob {
  if (job.status !== "running" || !job.lease) {
    throw new QueueError("invalid-data", "job is missing an active lease");
  }

  return { ...job, status: "running", lease: job.lease };
}

export class SqliteStore {
  private readonly database: DatabaseSync;

  constructor(filename: string, busyTimeoutMs: number) {
    this.database = openDatabase(filename, busyTimeoutMs);
  }

  close(): void {
    this.database.close();
  }

  private write<Result>(run: () => Result): Result {
    return writeTransaction(this.database, run);
  }

  private record(queue: string, jobId: string | null, event: EventName, now: number, details: JsonObject): void {
    this.database.prepare(`
      INSERT INTO queue_events (queue, job_id, event_name, occurred_at, details_json)
      VALUES (?, ?, ?, ?, ?)
    `).run(queue, jobId, event, now, JSON.stringify(details));
  }

  private findJob(jobId: string): Row | undefined {
    return this.database.prepare("SELECT * FROM jobs WHERE id = ?").get(jobId);
  }

  private insertJob(input: NormalizedEnqueue, now: number): EnqueueResult {
    const jobId = randomUUID();
    const inserted = this.database.prepare(`
      INSERT INTO jobs (
        id, queue, kind, payload_json, state, priority, available_at, attempts,
        max_attempts, retry_initial_ms, retry_max_ms, retry_multiplier,
        idempotency_key, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
    `).run(
      jobId,
      input.queue,
      input.kind,
      input.payloadJson,
      input.priority,
      input.availableAt,
      input.maxAttempts,
      input.retryPolicy.initialDelayMs,
      input.retryPolicy.maxDelayMs,
      input.retryPolicy.multiplier,
      input.idempotencyKey,
      now,
      now,
    );

    if (Number(inserted.changes) === 1) {
      this.record(input.queue, jobId, "enqueued", now, {
        kind: input.kind,
        priority: input.priority,
        availableAt: input.availableAt,
      });
      return { jobId, inserted: true };
    }

    const existing = this.database.prepare(`
      SELECT id FROM jobs WHERE queue = ? AND idempotency_key = ?
    `).get(input.queue, input.idempotencyKey);
    return { jobId: textColumn(requiredRow(existing), "id"), inserted: false };
  }

  enqueue(input: NormalizedEnqueue, now: number): EnqueueResult {
    return this.write(() => this.insertJob(input, now));
  }

  enqueueMany(inputs: NormalizedEnqueue[], now: number): EnqueueResult[] {
    return this.write(() => inputs.map((input) => this.insertJob(input, now)));
  }

  get(jobId: string): Job | null {
    const row = this.findJob(jobId);
    return row ? toJob(row) : null;
  }

  list(options: Required<Omit<ListJobsOptions, "status">> & Pick<ListJobsOptions, "status">): JobPage {
    const stateFilter = options.status === undefined ? "" : "AND state = ?";
    const parameters: SQLInputValue[] = [options.queue, options.after];
    if (options.status !== undefined) parameters.push(options.status);
    parameters.push(options.limit + 1);

    const rows = this.database.prepare(`
      SELECT * FROM jobs
      WHERE queue = ? AND ordinal > ? ${stateFilter}
      ORDER BY ordinal
      LIMIT ?
    `).all(...parameters);
    const page = rows.slice(0, options.limit);

    return {
      jobs: page.map(toJob),
      nextCursor: rows.length > options.limit ? numberColumn(page[page.length - 1], "ordinal") : null,
    };
  }

  private reclaim(now: number, options: ReclaimOptions & { limit: number }): ReclaimResult {
    const queued = options.queue === undefined ? "" : "AND queue = ?";
    const parameters: SQLInputValue[] = [now];
    if (options.queue !== undefined) parameters.push(options.queue);
    parameters.push(options.limit);

    const expired = this.database.prepare(`
      SELECT id, queue, attempts, max_attempts, worker_id, lease_until FROM jobs
      WHERE state = 'running' AND lease_until <= ? ${queued}
      ORDER BY lease_until, ordinal
      LIMIT ?
    `).all(...parameters);

    const requeue = this.database.prepare(`
      UPDATE jobs
      SET state = 'queued', available_at = ?, updated_at = ?,
          lease_token = NULL, worker_id = NULL, lease_until = NULL,
          last_error = 'Lease expired'
      WHERE id = ?
    `);
    const exhaust = this.database.prepare(`
      UPDATE jobs
      SET state = 'dead', updated_at = ?, finished_at = ?,
          lease_token = NULL, worker_id = NULL, lease_until = NULL,
          last_error = 'Lease expired'
      WHERE id = ?
    `);
    const result: ReclaimResult = { requeued: [], dead: [] };

    for (const row of expired) {
      const id = textColumn(row, "id");
      const queue = textColumn(row, "queue");
      const attempts = numberColumn(row, "attempts");
      const terminal = attempts >= numberColumn(row, "max_attempts");

      if (terminal) {
        exhaust.run(now, now, id);
        result.dead.push(id);
      } else {
        requeue.run(now, now, id);
        result.requeued.push(id);
      }

      this.record(queue, id, "lease-expired", now, {
        workerId: textColumn(row, "worker_id"),
        expiredAt: numberColumn(row, "lease_until"),
        attempts,
        state: terminal ? "dead" : "queued",
      });
    }

    return result;
  }

  reclaimExpired(options: ReclaimOptions & { limit: number }, now: number): ReclaimResult {
    return this.write(() => this.reclaim(now, options));
  }

  claim(options: Required<ClaimOptions>, now: number): ClaimedJob | null {
    return this.write(() => {
      if (this.getSettings(options.queue).paused) return null;

      this.reclaim(now, { queue: options.queue, limit: 100 });
      const row = this.database.prepare(`
        SELECT id FROM jobs
        WHERE queue = ? AND state = 'queued' AND available_at <= ? AND attempts < max_attempts
        ORDER BY priority DESC, available_at, ordinal
        LIMIT 1
      `).get(options.queue, now);
      if (!row) return null;

      const jobId = textColumn(row, "id");
      const token = randomUUID();
      const expiresAt = timeAfter(now, options.leaseMs);
      const updated = this.database.prepare(`
        UPDATE jobs
        SET state = 'running', lease_token = ?, worker_id = ?, lease_until = ?,
            attempts = attempts + 1, updated_at = ?
        WHERE id = ?
        RETURNING *
      `).get(token, options.workerId, expiresAt, now, jobId);

      const job = asClaim(toJob(requiredRow(updated)));
      this.record(job.queue, job.id, "claimed", now, {
        workerId: options.workerId,
        expiresAt,
        attempt: job.attempts,
      });
      return job;
    });
  }

  renew(lease: LeaseRef, leaseMs: number, now: number): ClaimedJob | null {
    return this.write(() => {
      const expiresAt = timeAfter(now, leaseMs);
      const row = this.database.prepare(`
        UPDATE jobs
        SET lease_until = max(lease_until, ?), updated_at = ?
        WHERE id = ? AND state = 'running' AND lease_token = ? AND lease_until > ?
        RETURNING *
      `).get(expiresAt, now, lease.jobId, lease.token, now);
      if (!row) return null;

      const job = asClaim(toJob(row));
      this.record(job.queue, job.id, "lease-renewed", now, {
        workerId: job.lease.workerId,
        expiresAt: job.lease.expiresAt,
      });
      return job;
    });
  }

  complete(lease: LeaseRef, now: number): boolean {
    return this.write(() => {
      const row = this.database.prepare(`
        UPDATE jobs
        SET state = 'succeeded', updated_at = ?, finished_at = ?, last_error = NULL,
            lease_token = NULL, worker_id = NULL, lease_until = NULL
        WHERE id = ? AND state = 'running' AND lease_until > ?
        RETURNING queue, attempts
      `).get(now, now, lease.jobId, now);
      if (!row) return false;

      this.record(textColumn(row, "queue"), lease.jobId, "succeeded", now, {
        attempt: numberColumn(row, "attempts"),
      });
      return true;
    });
  }

  fail(lease: LeaseRef, failure: FailOptions, now: number): FailResult {
    return this.write(() => {
      const row = this.database.prepare(`
        SELECT * FROM jobs
        WHERE id = ? AND state = 'running' AND lease_token = ? AND lease_until > ?
      `).get(lease.jobId, lease.token, now);
      if (!row) return { outcome: "lost" };

      const job = asClaim(toJob(row));
      const plan = planFailure(job, failure, now);
      const availableAt = plan.state === "queued" ? plan.availableAt : job.availableAt;
      const finishedAt = plan.state === "dead" ? now : null;

      this.database.prepare(`
        UPDATE jobs
        SET state = ?, updated_at = ?, finished_at = ?, last_error = ?, available_at = ?,
            lease_token = NULL, worker_id = NULL, lease_until = NULL
        WHERE id = ?
      `).run(plan.state, now, finishedAt, failure.message, availableAt, lease.jobId);

      if (plan.state === "dead") {
        this.record(job.queue, job.id, "dead", now, {
          attempt: job.attempts,
          message: failure.message,
        });
        return { outcome: "dead" };
      }

      this.record(job.queue, job.id, "retry-scheduled", now, {
        attempt: job.attempts,
        message: failure.message,
        availableAt,
      });
      return { outcome: "retry", availableAt };
    });
  }

  cancel(jobId: string, now: number): boolean {
    return this.write(() => {
      const row = this.database.prepare(`
        UPDATE jobs
        SET state = 'cancelled', updated_at = ?, finished_at = ?,
            lease_token = NULL, worker_id = NULL, lease_until = NULL
        WHERE id = ? AND state IN ('queued', 'running')
        RETURNING queue
      `).get(now, now, jobId);
      if (!row) return false;

      this.record(textColumn(row, "queue"), jobId, "cancelled", now, {});
      return true;
    });
  }

  retryDead(jobId: string, options: RetryDeadOptions & { availableAt: number }, now: number): Job | null {
    return this.write(() => {
      const row = this.database.prepare(`
        UPDATE jobs
        SET state = 'queued', available_at = ?, attempts = 0,
            max_attempts = coalesce(?, max_attempts), updated_at = ?,
            finished_at = NULL, last_error = NULL
        WHERE id = ? AND state = 'dead'
        RETURNING *
      `).get(options.availableAt, options.maxAttempts ?? null, now, jobId);
      if (!row) return null;

      const job = toJob(row);
      this.record(job.queue, job.id, "retried", now, {
        availableAt: job.availableAt,
        maxAttempts: job.maxAttempts,
      });
      return job;
    });
  }

  getSettings(queue: string): QueueSettings {
    const row = this.database.prepare("SELECT * FROM queue_settings WHERE queue = ?").get(queue);
    return toSettings(queue, row);
  }

  setPaused(queue: string, paused: boolean, now: number): QueueSettings {
    return this.write(() => {
      const current = this.getSettings(queue);
      if (current.paused === paused) return current;

      const row = this.database.prepare(`
        INSERT INTO queue_settings (queue, paused, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT (queue) DO UPDATE SET paused = excluded.paused, updated_at = excluded.updated_at
        RETURNING *
      `).get(queue, paused ? 1 : 0, now);

      this.record(queue, null, paused ? "queue-paused" : "queue-resumed", now, {});
      return toSettings(queue, requiredRow(row));
    });
  }

  status(queue: string, now: number): QueueStatus {
    const row = requiredRow(this.database.prepare(`
      SELECT
        coalesce((SELECT paused FROM queue_settings WHERE queue = ?), 0) AS paused,
        count(CASE WHEN state = 'queued' THEN 1 END) AS queued,
        count(CASE WHEN state = 'running' THEN 1 END) AS running,
        count(CASE WHEN state = 'succeeded' THEN 1 END) AS succeeded,
        count(CASE WHEN state = 'dead' THEN 1 END) AS dead,
        count(CASE WHEN state = 'cancelled' THEN 1 END) AS cancelled,
        count(CASE WHEN state = 'queued' AND available_at <= ? THEN 1 END) AS ready,
        count(CASE WHEN state = 'queued' AND available_at > ? THEN 1 END) AS delayed,
        count(CASE WHEN state = 'running' AND lease_until <= ? THEN 1 END) AS expired,
        min(CASE WHEN state = 'queued' THEN available_at END) AS next_available_at
      FROM jobs
      WHERE queue = ?
    `).get(queue, now, now, now, queue));

    const counts = {
      queued: 0,
      running: 0,
      succeeded: 0,
      dead: 0,
      cancelled: 0,
    };
    for (const status of jobStatuses) counts[status] = numberColumn(row, status);

    return {
      queue,
      paused: numberColumn(row, "paused") === 1,
      counts,
      ready: numberColumn(row, "ready"),
      delayed: numberColumn(row, "delayed"),
      expired: numberColumn(row, "expired"),
      nextAvailableAt: row.next_available_at === null ? null : numberColumn(row, "next_available_at"),
    };
  }

  readEvents(options: ReadEventsOptions & { after: number; limit: number }): QueueEvent[] {
    const conditions = ["sequence > ?"];
    const parameters: SQLInputValue[] = [options.after];

    if (options.queue !== undefined) {
      conditions.push("queue = ?");
      parameters.push(options.queue);
    }

    if (options.jobId !== undefined) {
      conditions.push("job_id = ?");
      parameters.push(options.jobId);
    }

    parameters.push(options.limit);

    return this.database.prepare(`
      SELECT * FROM queue_events
      WHERE ${conditions.join(" AND ")}
      ORDER BY sequence
      LIMIT ?
    `).all(...parameters).map(toEvent);
  }

  pruneEvents(options: PruneEventsOptions): number {
    return this.write(() => {
      const result = options.queue === undefined
        ? this.database.prepare("DELETE FROM queue_events WHERE occurred_at < ?").run(options.before)
        : this.database.prepare("DELETE FROM queue_events WHERE occurred_at < ? AND queue = ?")
          .run(options.before, options.queue);

      return Number(result.changes);
    });
  }
}

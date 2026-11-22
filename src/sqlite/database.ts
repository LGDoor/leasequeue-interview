import { DatabaseSync } from "node:sqlite";
import { QueueError } from "../errors.ts";

const migrations = [
  `
    CREATE TABLE jobs (
      ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      queue TEXT NOT NULL,
      kind TEXT NOT NULL,
      payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
      state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'dead', 'cancelled')),
      priority INTEGER NOT NULL,
      available_at INTEGER NOT NULL,
      attempts INTEGER NOT NULL CHECK (attempts >= 0),
      max_attempts INTEGER NOT NULL CHECK (max_attempts > 0 AND attempts <= max_attempts),
      retry_initial_ms INTEGER NOT NULL,
      retry_max_ms INTEGER NOT NULL,
      retry_multiplier REAL NOT NULL,
      idempotency_key TEXT,
      lease_token TEXT,
      worker_id TEXT,
      lease_until INTEGER,
      last_error TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      finished_at INTEGER,
      CHECK (
        (state = 'running' AND lease_token IS NOT NULL AND worker_id IS NOT NULL AND lease_until IS NOT NULL)
        OR
        (state <> 'running' AND lease_token IS NULL AND worker_id IS NULL AND lease_until IS NULL)
      ),
      CHECK (
        (state IN ('succeeded', 'dead', 'cancelled') AND finished_at IS NOT NULL)
        OR
        (state IN ('queued', 'running') AND finished_at IS NULL)
      )
    ) STRICT;

    CREATE UNIQUE INDEX jobs_idempotency ON jobs (queue, idempotency_key)
      WHERE idempotency_key IS NOT NULL;
    CREATE INDEX jobs_claimable ON jobs (queue, priority DESC, available_at, ordinal)
      WHERE state = 'queued';
    CREATE INDEX jobs_expiring ON jobs (lease_until, ordinal)
      WHERE state = 'running';
  `,
  `
    CREATE TABLE queue_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      queue TEXT NOT NULL,
      job_id TEXT REFERENCES jobs (id),
      event_name TEXT NOT NULL,
      occurred_at INTEGER NOT NULL,
      details_json TEXT NOT NULL CHECK (json_valid(details_json))
    ) STRICT;

    CREATE INDEX events_queue ON queue_events (queue, sequence);
    CREATE INDEX events_job ON queue_events (job_id, sequence);
    CREATE INDEX events_age ON queue_events (occurred_at);
  `,
  `
    CREATE TABLE queue_settings (
      queue TEXT PRIMARY KEY,
      paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
      updated_at INTEGER NOT NULL
    ) STRICT;

    CREATE INDEX jobs_queue ON jobs (queue, ordinal);
    CREATE INDEX jobs_queue_state ON jobs (queue, state, ordinal);
  `,
];

export const schemaVersion = migrations.length;

export function writeTransaction<Result>(database: DatabaseSync, run: () => Result): Result {
  database.exec("BEGIN IMMEDIATE");

  try {
    const result = run();
    database.exec("COMMIT");
    return result;
  } catch (error) {
    try {
      database.exec("ROLLBACK");
    } catch (rollbackError) {
      throw new AggregateError([error, rollbackError], "SQLite transaction and rollback failed");
    }

    throw error;
  }
}

function migrate(database: DatabaseSync): void {
  writeTransaction(database, () => {
    const version = database.prepare("PRAGMA user_version").get()?.user_version;

    if (typeof version !== "number" || version < 0 || version > schemaVersion) {
      throw new QueueError("invalid-schema", `unsupported SQLite schema version: ${String(version)}`);
    }

    for (let index = version; index < schemaVersion; index += 1) {
      database.exec(migrations[index]);
      database.exec(`PRAGMA user_version = ${index + 1}`);
    }
  });
}

export function openDatabase(filename: string, busyTimeoutMs: number): DatabaseSync {
  const database = new DatabaseSync(filename);

  try {
    database.exec(`PRAGMA busy_timeout = ${busyTimeoutMs}`);
    database.exec("PRAGMA foreign_keys = ON");
    migrate(database);
    database.exec("PRAGMA journal_mode = WAL");
    return database;
  } catch (error) {
    database.close();
    throw error;
  }
}

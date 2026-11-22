import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TestContext } from "node:test";
import { openQueue, QueueError } from "../src/index.ts";
import type {
  ClaimedJob,
  ClaimOptions,
  Clock,
  EnqueueInput,
  OpenQueueOptions,
  QueueClient,
  QueueErrorCode,
} from "../src/index.ts";

export function fakeClock(start = 10_000): Clock & {
  advance(durationMs: number): number;
  set(timestamp: number): void;
} {
  let timestamp = start;
  return {
    now() {
      return timestamp;
    },
    advance(durationMs) {
      timestamp += durationMs;
      return timestamp;
    },
    set(next) {
      timestamp = next;
    },
  };
}

export function memory(t: TestContext, start?: number) {
  const clock = fakeClock(start);
  const queue = openQueue(":memory:", { clock });
  t.after(() => queue.close());
  return { queue, clock };
}

export function disk(t: TestContext, start?: number) {
  const directory = mkdtempSync(join(tmpdir(), "leasequeue-test-"));
  const filename = join(directory, "queue.sqlite");
  const clock = fakeClock(start);
  const clients: QueueClient[] = [];

  t.after(() => {
    for (const client of clients.reverse()) {
      client.close();
    }
    rmSync(directory, { recursive: true, force: true });
  });

  return {
    filename,
    clock,
    open(options: OpenQueueOptions = {}) {
      const client = openQueue(filename, { clock, ...options });
      clients.push(client);
      return client;
    },
  };
}

export function input(options: Partial<EnqueueInput> = {}): EnqueueInput {
  return {
    queue: "reports",
    kind: "export",
    payload: { documentId: "week-14" },
    ...options,
  };
}

export function claim(queue: QueueClient, options: Partial<ClaimOptions> = {}): ClaimedJob {
  const job = queue.claim({ queue: "reports", workerId: "worker-one", ...options });
  assert.ok(job, "expected the worker to claim a job");
  return job;
}

export function hasCode(code: QueueErrorCode, message?: RegExp) {
  return (error: unknown): boolean => {
    assert.ok(error instanceof QueueError);
    assert.equal(error.code, code);
    if (message) assert.match(error.message, message);
    return true;
  };
}

export function deferred<Value = void>() {
  let resolve!: (value: Value | PromiseLike<Value>) => void;
  let reject!: (reason?: unknown) => void;

  const promise = new Promise<Value>((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });

  return { promise, resolve, reject };
}

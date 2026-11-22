import { setTimeout as delay } from "node:timers/promises";
import { integer, limits } from "./validate.ts";
import type { BatchResult, Worker } from "./worker.ts";

export type Wait = (durationMs: number, signal?: AbortSignal) => Promise<void>;

export type PollOptions = {
  signal?: AbortSignal;
  batchLimit?: number;
  idleDelayMs?: number;
  maxIterations?: number;
  onBatch?: (batch: BatchResult) => void | Promise<void>;
  wait?: Wait;
};

export type PollResult = {
  stopped: "aborted" | "limit";
  iterations: number;
  jobs: number;
  idleIterations: number;
};

async function waitWithTimer(durationMs: number, signal?: AbortSignal): Promise<void> {
  try {
    await delay(durationMs, undefined, { signal });
  } catch (error) {
    if (!signal?.aborted) throw error;
  }
}

export async function poll(worker: Worker, options: PollOptions = {}): Promise<PollResult> {
  const batchLimit = integer(options.batchLimit ?? 10, "batch limit", 1, limits.batch);
  const idleDelay = integer(options.idleDelayMs ?? 100, "idle delay", 1, 60_000);
  const maxIterations = integer(options.maxIterations ?? Number.MAX_SAFE_INTEGER, "iteration limit", 1, Number.MAX_SAFE_INTEGER);
  const wait = options.wait ?? waitWithTimer;
  let iterations = 0;
  let jobs = 0;
  let idleIterations = 0;

  while (iterations < maxIterations && !options.signal?.aborted) {
    const batch = await worker.runBatch({ limit: batchLimit, signal: options.signal });
    iterations += 1;
    jobs += batch.results.length;
    if (batch.results.length === 0) idleIterations += 1;
    await options.onBatch?.(batch);

    if (batch.drained && iterations < maxIterations && !options.signal?.aborted) {
      await wait(idleDelay, options.signal);
    }
  }

  return {
    stopped: options.signal?.aborted ? "aborted" : "limit",
    iterations,
    jobs,
    idleIterations,
  };
}

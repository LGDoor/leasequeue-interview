import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openQueue, poll, RetryAfterError, Worker } from "../src/index.ts";

const directory = mkdtempSync(join(tmpdir(), "leasequeue-demo-"));
let timestamp = 1_000;
const queue = openQueue(join(directory, "jobs.sqlite"), { clock: { now: () => timestamp } });

try {
  const exportJob = queue.enqueue({
    queue: "reports",
    kind: "export",
    payload: { report: "weekly-summary" },
    priority: 5,
  });
  const cleanupJob = queue.enqueue({
    queue: "reports",
    kind: "cleanup",
    payload: { target: "scratch" },
    availableAt: 1_100,
  });

  const worker = new Worker(queue, {
    queue: "reports",
    workerId: "demo-worker",
    leaseMs: 250,
    handlers: {
      export({ job }) {
        if (job.attempts === 1) throw new RetryAfterError("Report source is still warming up", 50);
        console.log("Exported report:", job.payload);
      },
      cleanup({ job }) {
        console.log("Cleaned demo workspace:", job.payload);
      },
    },
  });

  const loop = await poll(worker, {
    idleDelayMs: 50,
    maxIterations: 3,
    wait: async (durationMs) => { timestamp += durationMs; },
    onBatch(batch) {
      console.log(`t=${timestamp}:`, batch.results.map(({ outcome }) => outcome).join(", "));
    },
  });

  console.log("Polling:", loop);
  console.log("Final jobs:", [exportJob, cleanupJob].map(({ jobId }) => queue.get(jobId)?.status));
  console.log("Journal:", queue.readEvents({ queue: "reports" }).map(({ name }) => name).join(" → "));
} finally {
  queue.close();
  rmSync(directory, { recursive: true, force: true });
}

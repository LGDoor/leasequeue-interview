import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openQueue, PermanentJobError, poll, RetryAfterError, Worker } from "../src/index.ts";
import type { BatchResult, Clock, JsonValue, QueueClient } from "../src/index.ts";

type ReportRequest = {
  reportId: string;
  period: string;
  recipients: string[];
};

type DeliveryRequest = {
  reportId: string;
  period: string;
  recipient: string;
};

function objectPayload(payload: JsonValue) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new PermanentJobError("Expected a JSON object");
  }
  return payload;
}

function stringField(value: JsonValue | undefined, field: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new PermanentJobError(`Expected a nonblank ${field}`);
  }
  return value;
}

function reportPayload(payload: JsonValue): ReportRequest {
  const value = objectPayload(payload);
  if (!Array.isArray(value.recipients) || value.recipients.length === 0) {
    throw new PermanentJobError("Expected at least one recipient");
  }
  return {
    reportId: stringField(value.reportId, "report ID"),
    period: stringField(value.period, "period"),
    recipients: value.recipients.map((recipient) => stringField(recipient, "recipient")),
  };
}

function deliveryPayload(payload: JsonValue): DeliveryRequest {
  const value = objectPayload(payload);
  return {
    reportId: stringField(value.reportId, "report ID"),
    period: stringField(value.period, "period"),
    recipient: stringField(value.recipient, "recipient"),
  };
}

function manualClock(start: number): Clock & { advance(durationMs: number): void } {
  let now = start;
  return {
    now: () => now,
    advance(durationMs) {
      now += durationMs;
    },
  };
}

function generateReports(queue: QueueClient, artifacts: Map<string, string>): Worker {
  return new Worker(queue, {
    queue: "reports",
    workerId: "report-writer",
    handlers: {
      generate({ job }) {
        const { reportId, period, recipients } = reportPayload(job.payload);
        const artifactId = `${reportId}:${period}`;
        artifacts.set(artifactId, `report,period\n${reportId},${period}\n`);

        queue.enqueueMany(recipients.map((recipient) => ({
          queue: "deliveries",
          kind: "send",
          payload: { reportId, period, recipient },
          idempotencyKey: `${job.id}:${recipient}`,
        })));
      },
    },
  });
}

function deliverReports(
  queue: QueueClient,
  artifacts: Map<string, string>,
  receipts: Map<string, DeliveryRequest>,
): Worker {
  return new Worker(queue, {
    queue: "deliveries",
    workerId: "delivery-writer",
    handlers: {
      send({ job }) {
        const request = deliveryPayload(job.payload);
        const artifactId = `${request.reportId}:${request.period}`;
        if (!artifacts.has(artifactId)) {
          throw new PermanentJobError(`Missing report artifact ${artifactId}`);
        }

        if (request.recipient === "audit@example.invalid" && job.attempts === 1) {
          throw new RetryAfterError("Delivery service asked us to wait", 250);
        }

        receipts.set(job.id, request);
      },
    },
  });
}

function printBatch(label: string, batch: BatchResult): void {
  if (batch.results.length > 0) {
    console.log(label, batch.results.map((result) => result.outcome).join(", "));
  }
}

function printJournal(queue: QueueClient): void {
  let after = 0;
  const groups = new Map<string, Map<string, number>>();
  while (true) {
    const events = queue.readEvents({ after, limit: 4 });
    if (events.length === 0) break;

    for (const event of events) {
      const counts = groups.get(event.queue) ?? new Map<string, number>();
      counts.set(event.name, (counts.get(event.name) ?? 0) + 1);
      groups.set(event.queue, counts);
      after = event.sequence;
    }
  }

  for (const [name, counts] of groups) {
    console.log(`${name} journal:`, Object.fromEntries(counts));
  }
}

async function run(filename: string): Promise<void> {
  const clock = manualClock(Date.UTC(2026, 10, 20));
  const producer = openQueue(filename, { clock });
  try {
    const workerClient = openQueue(filename, { clock });
    try {
      await runPipeline(producer, workerClient, clock);
    } finally {
      workerClient.close();
    }
  } finally {
    producer.close();
  }
}

async function runPipeline(
  producer: QueueClient,
  workerClient: QueueClient,
  clock: ReturnType<typeof manualClock>,
): Promise<void> {
  const artifacts = new Map<string, string>();
  const receipts = new Map<string, DeliveryRequest>();
  const generator = generateReports(workerClient, artifacts);
  const sender = deliverReports(workerClient, artifacts, receipts);
  const scheduled = {
    queue: "reports",
    kind: "generate",
    idempotencyKey: "sales:2026-10",
    payload: {
      reportId: "sales",
      period: "2026-10",
      recipients: ["finance@example.invalid", "audit@example.invalid"],
    },
  };

  producer.setPaused("deliveries", true);
  console.log("New request:", producer.enqueue(scheduled).inserted);
  console.log("Producer replay:", producer.enqueue(scheduled).inserted);
  printBatch("Generation:", await generator.runBatch());
  console.log("Delivery status while paused:", producer.status("deliveries"));
  console.log("Paused worker:", (await sender.runOnce()).outcome);

  producer.setPaused("deliveries", false);
  const result = await poll(sender, {
    batchLimit: 4,
    idleDelayMs: 250,
    maxIterations: 3,
    async wait(durationMs) {
      clock.advance(durationMs);
    },
    onBatch: (batch) => printBatch("Delivery:", batch),
  });

  console.log("Poll result:", result);
  console.log("Delivery receipts:", [...receipts.values()]);
  printJournal(producer);
}

const directory = mkdtempSync(join(tmpdir(), "leasequeue-reports-"));
try {
  await run(join(directory, "queue.sqlite"));
} finally {
  rmSync(directory, { recursive: true, force: true });
}

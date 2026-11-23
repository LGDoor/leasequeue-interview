import { BufferedProducer, openQueue } from "../src/index.ts";
import type { EnqueueInput, EnqueueResult } from "../src/index.ts";

const regions = ["west", "east", "central", "north", "south", "coast"];

function reportRequests(period: string): EnqueueInput[] {
  return regions.map((region) => ({
    queue: "reports",
    kind: "export",
    payload: { region, period },
    idempotencyKey: `monthly:${region}:${period}`,
    priority: region === "central" ? 5 : 0,
  }));
}

function printResults(outcomes: PromiseSettledResult<EnqueueResult>[]): void {
  for (const [index, outcome] of outcomes.entries()) {
    if (outcome.status === "fulfilled") {
      const { jobId, inserted } = outcome.value;
      console.log(`#${index + 1}: ${inserted ? "inserted" : "reused"} ${jobId}`);
    } else {
      console.error(`#${index + 1}: rejected`, outcome.reason);
      process.exitCode = 1;
    }
  }
}

async function run(): Promise<void> {
  const client = openQueue(":memory:");
  const producer = new BufferedProducer(client, {
    batchSize: 3,
    maxPending: 12,
    lingerMs: 25,
  });

  try {
    const requests = reportRequests("2026-10");
    const alreadySubmitted = client.enqueue(requests[0]);
    console.log("Existing job:", alreadySubmitted.jobId);

    const recent = client.enqueue(requests[0]);
    console.log("Direct replay inserted a job:", recent.inserted);

    const submissions = requests.map((request) => producer.enqueue(request));
    const notified = producer.enqueue({
      queue: "notifications",
      kind: "send-summary",
      payload: { period: "2026-10", reportCount: requests.length },
      idempotencyKey: "summary:2026-10",
    });
    const finished = Promise.allSettled([...submissions, notified]);

    console.log("Before dispatch:", producer.getStats());
    console.log("Drain summary:", await producer.close());
    printResults(await finished);
    console.log("Final producer statistics:", producer.getStats());

    for (const name of ["reports", "notifications"]) {
      const status = client.status(name);
      console.log(`${name}: ${status.ready} ready, ${status.delayed} delayed`);
    }
  } finally {
    await producer.close();
    client.close();
  }
}

await run();

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, truncateSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, test } from "node:test";
import { runCli } from "../src/cli.ts";
import { leaseRef } from "../src/index.ts";
import type {
  EnqueueResult,
  Job,
  JobPage,
  QueueEvent,
  QueueSettings,
  QueueStatus,
  ReclaimResult,
} from "../src/index.ts";
import { claim, disk, input } from "./helpers.ts";

function invoke(args: string[]) {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const code = runCli(args, {
    stdout: (message) => stdout.push(message),
    stderr: (message) => stderr.push(message),
  });
  return { code, stdout, stderr };
}

function call<Result>(command: string, filename: string, ...args: string[]): Result {
  const result = invoke([command, "--db", filename, ...args]);
  assert.equal(result.code, 0, result.stderr.join("\n"));
  assert.equal(result.stdout.length, 1);
  assert.deepEqual(result.stderr, []);
  return JSON.parse(result.stdout[0]) as Result;
}

describe("operator CLI input", () => {
  for (const args of [[], ["--help"], ["list", "--help"]]) {
    test(`prints usage for ${JSON.stringify(args)}`, () => {
      const result = invoke(args);
      assert.equal(result.code, 0);
      assert.equal(result.stdout.length, 1);
      assert.match(result.stdout[0], /LeaseQueue operator CLI/);
      assert.deepEqual(result.stderr, []);
    });
  }

  const invalid: { label: string; args: string[]; error: RegExp }[] = [
    { label: "unknown command", args: ["launch"], error: /unknown command/ },
    { label: "inherited property name", args: ["constructor"], error: /unknown command/ },
    { label: "missing database flag", args: ["status", "--queue", "reports"], error: /missing --db/ },
    { label: "unsupported option", args: ["status", "--db", ":memory:", "--job", "123"], error: /--job/ },
    { label: "missing queue", args: ["status", "--db", ":memory:"], error: /missing --queue/ },
    {
      label: "duplicate option",
      args: ["list", "--db", ":memory:", "--queue", "reports", "--queue", "deliveries"],
      error: /--queue was supplied more than once/,
    },
    {
      label: "invalid page size",
      args: ["list", "--db", ":memory:", "--queue", "reports", "--limit", "1.5"],
      error: /--limit must be a safe integer/,
    },
    {
      label: "unsafe integer",
      args: ["prune", "--db", ":memory:", "--before", "9007199254740992"],
      error: /--before must be a safe integer/,
    },
    {
      label: "invalid state filter",
      args: ["list", "--db", ":memory:", "--queue", "reports", "--status", "completed"],
      error: /status must be a supported job status/,
    },
  ];

  for (const { label, args, error } of invalid) {
    test(`rejects ${label} without a success result`, () => {
      const result = invoke(args);
      assert.equal(result.code, 1);
      assert.deepEqual(result.stdout, []);
      assert.equal(result.stderr.length, 1);
      assert.match(result.stderr[0], /^leasequeue:/);
      assert.match(result.stderr[0], error);
    });
  }

  test("does not open a file when the enqueue payload cannot be parsed", (context) => {
    const { filename } = disk(context);
    const result = invoke([
      "enqueue", "--db", filename, "--queue", "reports", "--kind", "export", "--payload", "not json",
    ]);

    assert.equal(result.code, 1);
    assert.match(result.stderr[0], /--payload must be valid JSON/);
    assert.equal(existsSync(filename), false);
  });

  test("returns a failure status for a path whose parent does not exist", (context) => {
    const { filename } = disk(context);
    const result = invoke(["status", "--db", `${filename}/missing.sqlite`, "--queue", "reports"]);

    assert.equal(result.code, 1);
    assert.deepEqual(result.stdout, []);
    assert.equal(result.stderr.length, 1);
  });
});

describe("operator workflows", () => {
  test("enqueues JSON and exposes the same durable job through the client API", (context) => {
    const { filename, open } = disk(context, 200);
    const client = open();
    const args = [
      "--queue", "reports", "--kind", "export", "--payload", '{"month":"2026-10","regions":["US","EU"]}',
      "--key", "sales:2026-10", "--priority=-5", "--available-at", "500", "--max-attempts", "7",
    ];

    const submitted = call<EnqueueResult>("enqueue", filename, ...args);
    assert.equal(submitted.inserted, true);
    const job = call<Job>("get", filename, "--job", submitted.jobId);
    assert.deepEqual(job, client.get(submitted.jobId));
    assert.deepEqual(job.payload, { month: "2026-10", regions: ["US", "EU"] });
    assert.equal(job.priority, -5);
    assert.equal(job.availableAt, 500);
    assert.equal(job.maxAttempts, 7);
    assert.equal(job.attempts, 0);
    assert.deepEqual(call<EnqueueResult>("enqueue", filename, ...args), {
      jobId: submitted.jobId, inserted: false,
    });
    assert.equal(client.readEvents({ jobId: submitted.jobId }).length, 1);
  });

  test("preserves scalar payloads through the CLI", (context) => {
    const { filename, open } = disk(context);
    const client = open();

    for (const scalar of [null, false, 0, ""]) {
      const { jobId } = call<EnqueueResult>("enqueue", filename,
        "--queue", "reports", "--kind", "export", "--payload", JSON.stringify(scalar));
      assert.equal(client.get(jobId)?.payload, scalar);
    }
  });

  test("uses page cursors and filters when listing jobs", (context) => {
    const { filename, open } = disk(context);
    const client = open();
    const [first, second, third] = client.enqueueMany([
      input(), input(), input({ queue: "deliveries" }),
    ]);
    client.cancel(second.jobId);

    const firstPage = call<JobPage>("list", filename, "--queue", "reports", "--limit", "1");
    assert.deepEqual(firstPage.jobs.map(({ id }) => id), [first.jobId]);
    assert.ok(firstPage.nextCursor);
    const lastPage = call<JobPage>("list", filename, "--queue", "reports", "--after", String(firstPage.nextCursor));
    assert.deepEqual(lastPage.jobs.map(({ id }) => id), [second.jobId]);
    assert.equal(lastPage.nextCursor, null);
    const cancelled = call<JobPage>("list", filename, "--queue", "reports", "--status", "cancelled");
    assert.deepEqual(cancelled.jobs.map(({ id }) => id), [second.jobId]);
    const delivery = call<JobPage>("list", filename, "--queue", "deliveries");
    assert.deepEqual(delivery.jobs.map(({ id }) => id), [third.jobId]);
  });

  test("pauses and resumes work visible to a running client", (context) => {
    const { filename, open } = disk(context);
    const worker = open();
    const { jobId } = worker.enqueue(input());

    const paused = call<QueueSettings>("pause", filename, "--queue", "reports");
    assert.equal(paused.paused, true);
    assert.deepEqual(worker.getSettings("reports"), paused);
    assert.equal(worker.claim({ queue: "reports", workerId: "worker-one" }), null);
    const status = call<QueueStatus>("status", filename, "--queue", "reports");
    assert.equal(status.paused, true);
    assert.equal(status.ready, 1);

    assert.equal(call<QueueSettings>("resume", filename, "--queue", "reports").paused, false);
    assert.equal(claim(worker).id, jobId);
    assert.deepEqual(worker.readEvents({ jobId }).map(({ name }) => name), ["enqueued", "claimed"]);
  });

  test("formats absence and no-op transitions as JSON, not command errors", (context) => {
    const { filename } = disk(context);

    assert.equal(call("get", filename, "--job", "not-found"), null);
    assert.equal(call("cancel", filename, "--job", "not-found"), false);
    assert.equal(call("retry", filename, "--job", "not-found"), null);
    assert.deepEqual(call("events", filename, "--job", "not-found"), []);
  });

  test("retries a dead job with an operator-selected date and attempt budget", (context) => {
    const { filename, open, clock } = disk(context, 100);
    const worker = open();
    const { jobId } = worker.enqueue(input({ maxAttempts: 1 }));
    worker.fail(leaseRef(claim(worker)), { message: "format server offline" });

    const retried = call<Job>("retry", filename, "--job", jobId, "--max-attempts", "3", "--available-at", "300");
    assert.equal(retried.id, jobId);
    assert.equal(retried.status, "queued");
    assert.equal(retried.attempts, 0);
    assert.equal(retried.maxAttempts, 3);
    assert.equal(retried.availableAt, 300);
    assert.equal(worker.status("reports").delayed, 1);
    clock.set(300);
    assert.equal(claim(worker).id, jobId);
  });

  test("reads journal pages and prunes history without deleting jobs", (context) => {
    const { filename, open, clock } = disk(context, 100);
    const client = open();
    const { jobId } = client.enqueue(input());
    const first = call<QueueEvent[]>("events", filename, "--job", jobId, "--limit", "1");
    assert.equal(first[0].name, "enqueued");
    clock.set(200);
    assert.equal(call("cancel", filename, "--job", jobId), true);

    const later = call<QueueEvent[]>("events", filename, "--job", jobId, "--after", String(first[0].sequence));
    assert.deepEqual(later.map(({ name }) => name), ["cancelled"]);
    assert.deepEqual(call("prune", filename, "--before", "150", "--queue", "reports"), { deleted: 1 });
    assert.deepEqual(call("events", filename, "--job", jobId), later);
    assert.equal(client.get(jobId)?.status, "cancelled");
    assert.equal(call("cancel", filename, "--job", jobId), false);
  });

  test("reclaims only the requested queue in a bounded sweep", (context) => {
    const { filename, open } = disk(context, 100);
    const client = open();
    const [first, second, other] = client.enqueueMany([
      input({ maxAttempts: 1 }), input(), input({ queue: "deliveries" }),
    ]);
    claim(client, { leaseMs: 10 });
    claim(client, { leaseMs: 20 });
    claim(client, { queue: "deliveries", leaseMs: 1 });

    assert.deepEqual(call<ReclaimResult>("reclaim", filename, "--queue", "reports", "--limit", "1"), {
      requeued: [], dead: [first.jobId],
    });
    assert.equal(client.get(second.jobId)?.status, "running");
    assert.equal(client.get(other.jobId)?.status, "running");
    assert.deepEqual(call<ReclaimResult>("reclaim", filename, "--queue", "reports"), {
      requeued: [second.jobId], dead: [],
    });
  });
});

describe("process entry point", () => {
  const cli = fileURLToPath(new URL(`../src/cli.${import.meta.url.endsWith(".ts") ? "ts" : "js"}`, import.meta.url));

  test("returns machine-readable stdout when invoked as a program", (context) => {
    const { filename } = disk(context);
    const result = spawnSync(process.execPath, [
      "--disable-warning=ExperimentalWarning", cli, "status", "--db", filename, "--queue", "reports",
    ], { encoding: "utf8" });

    assert.equal(result.error, undefined);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stderr, "");
    const status = JSON.parse(result.stdout) as QueueStatus;
    assert.equal(status.queue, "reports");
    assert.equal(status.nextAvailableAt, null);
    assert.equal(status.counts.queued, 0);
  });

  test("sets a nonzero process exit code and writes errors to stderr", () => {
    const result = spawnSync(process.execPath, [
      "--disable-warning=ExperimentalWarning", cli, "status", "--db", ":memory:",
    ], { encoding: "utf8" });

    assert.equal(result.error, undefined);
    assert.equal(result.status, 1);
    assert.equal(result.stdout, "");
    assert.match(result.stderr, /missing --queue/);
  });
});

describe("bulk import", () => {
  type Imported = { inserted: number; reused: number; results: EnqueueResult[] };

  function manifest(filename: string, contents: string | Uint8Array): string {
    const path = join(dirname(filename), "jobs.ndjson");
    writeFileSync(path, contents);
    return path;
  }

  function importArgs(filename: string, path: string): string[] {
    return ["import", "--db", filename, "--queue", "reports", "--file", path];
  }

  test("imports mixed job kinds and retains the order of results on replay", (context) => {
    const { filename, open } = disk(context);
    const path = manifest(filename, [
      '{"kind":"export","payload":{"report":"daily"},"idempotencyKey":"daily-30"}',
      "",
      '{"kind":"notify","payload":["finance"],"idempotencyKey":"daily-30-mail","priority":5}',
      '{"kind":"refresh","payload":null}',
    ].join("\r\n"));

    const result = call<Imported>("import", filename, "--queue", "reports", "--file", path);
    assert.equal(result.inserted, 3);
    assert.equal(result.reused, 0);
    const client = open();
    assert.deepEqual(result.results.map(({ jobId }) => client.get(jobId)?.kind), ["export", "notify", "refresh"]);
    assert.equal(client.get(result.results[1].jobId)?.priority, 5);

    const replay = call<Imported>("import", filename, "--queue", "reports", "--file", path);
    assert.equal(replay.inserted, 1);
    assert.equal(replay.reused, 2);
    assert.deepEqual(replay.results.slice(0, 2), result.results.slice(0, 2).map(({ jobId }) => ({
      jobId, inserted: false,
    })));
    assert.notEqual(replay.results[2].jobId, result.results[2].jobId);
    assert.equal(client.readEvents({ queue: "reports" }).length, 4);
  });

  test("accepts optional schedule and retry settings", (context) => {
    const { filename, open } = disk(context);
    const path = manifest(filename, JSON.stringify({
      kind: "export",
      payload: { text: "财务" },
      availableAt: 123_456,
      maxAttempts: 3,
      retryPolicy: { initialDelayMs: 100, maxDelayMs: 600, multiplier: 1.5 },
    }));

    const { results: [{ jobId }] } = call<Imported>("import", filename, "--queue", "reports", "--file", path);
    const job = open().get(jobId);
    assert.deepEqual(job?.payload, { text: "财务" });
    assert.equal(job?.availableAt, 123_456);
    assert.equal(job?.maxAttempts, 3);
    assert.deepEqual(job?.retryPolicy, { initialDelayMs: 100, maxDelayMs: 600, multiplier: 1.5 });
  });

  test("reports malformed JSON at its original line number", (context) => {
    const { filename } = disk(context);
    const path = manifest(filename, '\n{"kind":"export","payload":null}\n\nnot json\n');
    const result = invoke(importArgs(filename, path));

    assert.equal(result.code, 1);
    assert.match(result.stderr[0], /line 4 must be valid JSON/);
    assert.deepEqual(result.stdout, []);
    assert.equal(existsSync(filename), false);
  });

  for (const { contents, error } of [
    { contents: "[]", error: /line 1 must be an object/ },
    { contents: '{"kind":"export"}', error: /line 1.payload is required/ },
    { contents: '{"kind":"export","payload":0,"queue":"elsewhere"}', error: /line 1.*unsupported field: queue/ },
    { contents: '{"kind":"export","payload":0,"priority":"5"}', error: /line 1.priority must be a number/ },
    { contents: '{"kind":"export","payload":0,"idempotencyKey":10}', error: /line 1.idempotencyKey must be a string/ },
    { contents: '{"kind":"export","payload":0,"retryPolicy":null}', error: /line 1.retryPolicy must be an object/ },
    { contents: '{"kind":"export","payload":0,"retryPolicy":{"jitter":1}}', error: /unsupported field: jitter/ },
  ]) {
    test(`rejects unsupported manifest input: ${contents}`, (context) => {
      const { filename } = disk(context);
      const result = invoke(importArgs(filename, manifest(filename, contents)));

      assert.equal(result.code, 1);
      assert.match(result.stderr[0], error);
      assert.equal(existsSync(filename), false);
    });
  }

  test("does not leave partial inserts if a parsed job fails queue validation", (context) => {
    const { filename, open } = disk(context);
    const client = open();
    const prior = client.enqueue(input());
    const path = manifest(filename, [
      '{"kind":"export","payload":{"prefix":true}}',
      '{"kind":"export","payload":null,"maxAttempts":0}',
    ].join("\n"));

    const result = invoke(importArgs(filename, path));
    assert.equal(result.code, 1);
    assert.match(result.stderr[0], /maxAttempts/);
    assert.deepEqual(client.list({ queue: "reports" }).jobs.map(({ id }) => id), [prior.jobId]);
    assert.equal(client.readEvents().length, 1);
  });

  test("rejects a manifest beyond the batch limit", (context) => {
    const { filename } = disk(context);
    const line = '{"kind":"export","payload":null}\n';
    const result = invoke(importArgs(filename, manifest(filename, line.repeat(201))));

    assert.equal(result.code, 1);
    assert.match(result.stderr[0], /may contain at most 200 jobs/);
    assert.equal(existsSync(filename), false);
  });

  test("rejects an oversized file before decoding it", (context) => {
    const { filename } = disk(context);
    const path = manifest(filename, "");
    truncateSync(path, 16 * 1024 * 1024 + 1);
    const result = invoke(importArgs(filename, path));

    assert.equal(result.code, 1);
    assert.match(result.stderr[0], /may not exceed 16 MiB/);
    assert.equal(existsSync(filename), false);
  });

  test("rejects broken UTF-8 rather than replacing characters", (context) => {
    const { filename } = disk(context);
    const path = manifest(filename, Uint8Array.of(0xc3, 0x28));
    const result = invoke(importArgs(filename, path));
    assert.equal(result.code, 1);
    assert.match(result.stderr[0], /must contain UTF-8 text/);
  });

  test("treats a blank manifest as an empty batch", (context) => {
    const { filename, open } = disk(context);
    const path = manifest(filename, "\n\t\r\n");

    assert.deepEqual(call<Imported>("import", filename, "--queue", "reports", "--file", path), {
      inserted: 0, reused: 0, results: [],
    });
    assert.deepEqual(open().readEvents(), []);
  });
});

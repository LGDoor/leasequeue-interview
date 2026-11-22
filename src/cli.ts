import { closeSync, constants, fstatSync, openSync, readSync } from "node:fs";
import { parseArgs } from "node:util";
import { openQueue, QueueError } from "./index.ts";
import type { EnqueueInput, JobStatus, JsonValue, QueueClient, RetryPolicy } from "./index.ts";
import { limits, name } from "./validate.ts";

const usage = `LeaseQueue operator CLI

Usage: npm run --silent queue -- <command> --db <filename> [options]

  enqueue  --queue <name> --kind <name> --payload <json>
           [--key <key>] [--priority <number>] [--available-at <milliseconds>]
           [--max-attempts <number>]
  import   --queue <name> --file <ndjson-path>
  get      --job <id>
  list     --queue <name> [--status <state>] [--after <cursor>] [--limit <number>]
  status   --queue <name>
  events   [--queue <name>] [--job <id>] [--after <sequence>] [--limit <number>]
  pause    --queue <name>
  resume   --queue <name>
  cancel   --job <id>
  retry    --job <id> [--available-at <milliseconds>] [--max-attempts <number>]
  reclaim  [--queue <name>] [--limit <number>]
  prune    --before <milliseconds> [--queue <name>]

Timestamps are Unix milliseconds. Results are written as JSON to stdout.
Use --help to display this message without opening a database.`;

const commandOptions = {
  enqueue: ["queue", "kind", "payload", "key", "priority", "available-at", "max-attempts"],
  import: ["queue", "file"],
  get: ["job"],
  list: ["queue", "status", "after", "limit"],
  status: ["queue"],
  events: ["queue", "job", "after", "limit"],
  pause: ["queue"],
  resume: ["queue"],
  cancel: ["job"],
  retry: ["job", "available-at", "max-attempts"],
  reclaim: ["queue", "limit"],
  prune: ["before", "queue"],
} as const;

type Command = keyof typeof commandOptions;
type Flags = Record<string, string | undefined>;
type Action = (queue: QueueClient) => unknown;

function badInput(message: string): never {
  throw new QueueError("invalid-input", message);
}

function required(flags: Flags, key: string): string {
  const value = flags[key];
  if (value === undefined) badInput(`missing --${key}`);
  return value;
}

function parseInteger(value: string, key: string): number {
  const result = Number(value);
  if (!/^-?\d+$/.test(value) || !Number.isSafeInteger(result)) {
    badInput(`--${key} must be a safe integer`);
  }
  return result;
}

function optionalInteger(flags: Flags, key: string): number | undefined {
  const value = flags[key];
  return value === undefined ? undefined : parseInteger(value, key);
}

function jsonPayload(flags: Flags): JsonValue {
  const payload = required(flags, "payload");
  try {
    return JSON.parse(payload) as JsonValue;
  } catch {
    return badInput("--payload must be valid JSON");
  }
}

const importOptions = new Set([
  "kind", "payload", "idempotencyKey", "priority", "availableAt", "maxAttempts", "retryPolicy",
]);
const policyOptions = new Set(["initialDelayMs", "maxDelayMs", "multiplier"]);
const maxImportBytes = 16 * 1024 * 1024;

function importObject(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    badInput(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function importNumber(value: unknown, label: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) badInput(`${label} must be a number`);
  return value;
}

function importPolicy(value: unknown, label: string): Partial<RetryPolicy> | undefined {
  if (value === undefined) return undefined;
  const object = importObject(value, label);

  for (const field of Object.keys(object)) {
    if (!policyOptions.has(field)) badInput(`${label} contains an unsupported field: ${field}`);
  }

  return {
    initialDelayMs: importNumber(object.initialDelayMs, `${label}.initialDelayMs`),
    maxDelayMs: importNumber(object.maxDelayMs, `${label}.maxDelayMs`),
    multiplier: importNumber(object.multiplier, `${label}.multiplier`),
  };
}

function importJob(queue: string, value: unknown, line: number): EnqueueInput {
  const label = `line ${line}`;
  const object = importObject(value, label);

  for (const field of Object.keys(object)) {
    if (!importOptions.has(field)) badInput(`${label} contains an unsupported field: ${field}`);
  }

  if (typeof object.kind !== "string") badInput(`${label}.kind must be a string`);
  if (!Object.hasOwn(object, "payload")) badInput(`${label}.payload is required`);
  if (object.idempotencyKey !== undefined && typeof object.idempotencyKey !== "string") {
    badInput(`${label}.idempotencyKey must be a string`);
  }

  return {
    queue,
    kind: object.kind,
    payload: object.payload as JsonValue,
    idempotencyKey: object.idempotencyKey,
    priority: importNumber(object.priority, `${label}.priority`),
    availableAt: importNumber(object.availableAt, `${label}.availableAt`),
    maxAttempts: importNumber(object.maxAttempts, `${label}.maxAttempts`),
    retryPolicy: importPolicy(object.retryPolicy, `${label}.retryPolicy`),
  };
}

function readImportFile(filename: string): string {
  const handle = openSync(filename, constants.O_RDONLY | constants.O_NONBLOCK);
  try {
    const stats = fstatSync(handle);
    if (!stats.isFile()) badInput("--file must point to a regular file");
    if (stats.size > maxImportBytes) badInput("--file may not exceed 16 MiB");

    const bytes = Buffer.allocUnsafe(stats.size + 1);
    let offset = 0;
    while (offset < bytes.length) {
      const count = readSync(handle, bytes, offset, bytes.length - offset, null);
      if (count === 0) break;
      offset += count;
    }
    if (offset !== stats.size) badInput("--file changed while it was being read");

    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, offset));
    } catch {
      return badInput("--file must contain UTF-8 text");
    }
  } finally {
    closeSync(handle);
  }
}

function readImport(queue: string, filename: string): EnqueueInput[] {
  const jobs: EnqueueInput[] = [];
  const lines = readImportFile(filename).split(/\r?\n/);

  for (const [index, source] of lines.entries()) {
    if (!source.trim()) continue;
    if (jobs.length === limits.batch) badInput(`--file may contain at most ${limits.batch} jobs`);

    const line = index + 1;
    let value: unknown;
    try {
      value = JSON.parse(source);
    } catch {
      badInput(`line ${line} must be valid JSON`);
    }
    jobs.push(importJob(queue, value, line));
  }

  return jobs;
}

function readAction(command: Command, flags: Flags): Action {
  switch (command) {
    case "enqueue": {
      const input = {
        queue: required(flags, "queue"),
        kind: required(flags, "kind"),
        payload: jsonPayload(flags),
        idempotencyKey: flags.key,
        priority: optionalInteger(flags, "priority"),
        availableAt: optionalInteger(flags, "available-at"),
        maxAttempts: optionalInteger(flags, "max-attempts"),
      };
      return (queue) => queue.enqueue(input);
    }
    case "import": {
      const jobs = readImport(name(required(flags, "queue"), "queue"), required(flags, "file"));
      return (queue) => {
        const results = queue.enqueueMany(jobs);
        const inserted = results.filter((result) => result.inserted).length;
        return { inserted, reused: results.length - inserted, results };
      };
    }
    case "get": {
      const id = required(flags, "job");
      return (queue) => queue.get(id);
    }
    case "list": {
      const options = {
        queue: required(flags, "queue"),
        status: flags.status as JobStatus | undefined,
        after: optionalInteger(flags, "after"),
        limit: optionalInteger(flags, "limit"),
      };
      return (queue) => queue.list(options);
    }
    case "status": {
      const name = required(flags, "queue");
      return (queue) => queue.status(name);
    }
    case "events": {
      const options = {
        queue: flags.queue,
        jobId: flags.job,
        after: optionalInteger(flags, "after"),
        limit: optionalInteger(flags, "limit"),
      };
      return (queue) => queue.readEvents(options);
    }
    case "pause":
    case "resume": {
      const name = required(flags, "queue");
      return (queue) => queue.setPaused(name, command === "pause");
    }
    case "cancel": {
      const id = required(flags, "job");
      return (queue) => queue.cancel(id);
    }
    case "retry": {
      const id = required(flags, "job");
      const options = {
        availableAt: optionalInteger(flags, "available-at"),
        maxAttempts: optionalInteger(flags, "max-attempts"),
      };
      return (queue) => queue.retryDead(id, options);
    }
    case "reclaim": {
      const options = { queue: flags.queue, limit: optionalInteger(flags, "limit") };
      return (queue) => queue.reclaimExpired(options);
    }
    case "prune": {
      const before = parseInteger(required(flags, "before"), "before");
      return (queue) => ({ deleted: queue.pruneEvents({ before, queue: flags.queue }) });
    }
  }
}

export type CliOutput = {
  stdout(message: string): void;
  stderr(message: string): void;
};

export function runCli(args: readonly string[], output: CliOutput): number {
  if (args.length === 0 || args[0] === "--help") {
    output.stdout(usage);
    return 0;
  }

  try {
    const [command, ...rest] = args;
    if (!Object.hasOwn(commandOptions, command)) badInput(`unknown command: ${command}`);
    if (rest.length === 1 && rest[0] === "--help") {
      output.stdout(usage);
      return 0;
    }

    const selected = command as Command;
    const options = Object.fromEntries(
      ["db", ...commandOptions[selected]].map((key) => [key, { type: "string" as const }]),
    );
    const { values, tokens } = parseArgs({ args: rest, options, tokens: true });
    const seen = new Set<string>();
    for (const token of tokens) {
      if (token.kind !== "option") continue;
      if (seen.has(token.name)) badInput(`--${token.name} was supplied more than once`);
      seen.add(token.name);
    }

    const flags = values as Flags;
    const filename = required(flags, "db");
    const action = readAction(selected, flags);
    const queue = openQueue(filename);
    let result: unknown;
    try {
      result = action(queue);
    } finally {
      queue.close();
    }
    output.stdout(JSON.stringify(result, null, 2));
    return 0;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    output.stderr(`leasequeue: ${message}`);
    return 1;
  }
}

if (import.meta.main) {
  process.exitCode = runCli(process.argv.slice(2), {
    stdout: (message) => process.stdout.write(`${message}\n`),
    stderr: (message) => process.stderr.write(`${message}\n`),
  });
}

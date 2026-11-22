# LeaseQueue

LeaseQueue is a small TypeScript library for durable background jobs on a single
host. Producers, workers, and operator tools share a SQLite database. Job
handlers are ordinary functions supplied by the application; the library owns
job state, leases, retry scheduling, and a journal of state changes.

## Run locally

Use Node.js 24, version 24.2 or newer. The library and tests use built-in Node
modules, so the test and demo commands do not need a dependency install.

```sh
node --version
npm test
npm run example
```

The optional type check and build use the compiler pinned in the lockfile:

```sh
npm ci
npm run build
```

The build emits JavaScript and declaration files under `dist/`. To run one test
file without package scripts:

```sh
node --disable-warning=ExperimentalWarning --test test/lifecycle.test.ts
```

## At a glance

```ts
import { openQueue, Worker } from "./src/index.ts";

const queue = openQueue(":memory:");

try {
  queue.enqueue({
    queue: "reports",
    kind: "export",
    payload: { reportId: "weekly-sales" },
    idempotencyKey: "weekly-sales:2026-11-16",
  });

  const worker = new Worker(queue, {
    queue: "reports",
    workerId: "reports-one",
    handlers: {
      export({ job }) {
        console.log("Generating report", job.payload);
      },
    },
  });

  console.log(await worker.runOnce());
} finally {
  queue.close();
}
```

Use a file path instead of `:memory:` when producers and workers need to share
state. For a file-backed example with two processing stages and a transient
delivery failure, run `npm run example:reports`.

## Repository map

| Area | Responsibility |
| --- | --- |
| `src/index.ts`, `src/model.ts` | Public exports, input and result types |
| `src/client.ts`, `src/validate.ts` | Client operations and input validation |
| `src/sqlite/` | Schema, transactions, storage queries, row decoding |
| `src/worker.ts`, `src/scheduler.ts` | Handler execution, batch processing, polling |
| `src/clock.ts`, `src/retry.ts`, `src/events.ts` | Time, retry policy, event vocabulary |
| `src/cli.ts`, `examples/` | Operator commands and runnable examples |
| `test/` | Deterministic tests using in-memory and temporary databases |

- [API reference](docs/api.md): method contracts, result types, and limits.
- [Operations guide](docs/operations.md): files, recovery, and CLI recipes.

## Interview workspace

Use `main` as the starting point for the assigned issue. Review the assigned
pull request separately so its changes don’t affect your issue work. Keep code
changes and review notes local; don’t push branches or post review comments to
the shared repository. Be ready to explain what you verified and which checks
you ran. The examples are ordinary TypeScript; you don’t need a framework or an
external service to work on the repository.

### If you are new to TypeScript

Most files are ordinary JavaScript with types added to describe inputs and
results. `import type` only brings in type information; it does not run code.
`?` after a field name means callers may omit that field. A union such as
`{ outcome: "idle" } | { outcome: "succeeded"; jobId: string }` means you can
check `outcome` before reading the fields specific to that result.

Start by running the tests. You can read their assertions as usage examples
and rerun a single file while you work. Use `npm run build` to check types if
you installed the compiler; `npm test` checks runtime behavior without it.

For the pull request, read the base-to-head diff rather than switching your
issue branch to the review branch. Record any findings locally with the
behavior you observed or expect, and how you would verify it.

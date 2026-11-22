# Operations guide

## Pick a database location

Each deployment should use one file-backed database per application. One file
can hold several queues. Use a stable absolute path for each producer and
worker, and ensure that processes can write to the containing directory.
`:memory:` is useful for isolated tests, not for coordination across clients.

SQLite may maintain `-wal` and `-shm` companions alongside an open database.
Treat those files as part of the active database, not as temporary debris to
remove. Use a SQLite-aware backup procedure or stop clients before copying the
database. Verify a restore on a separate path before resuming production.

Opening a client applies schema migrations. When upgrading the library,
rehearse the upgrade against a copy and confirm that all deployed clients
understand the new schema before restarting them. An older library rejects a
database with a newer schema rather than trying to downgrade it.

## Inspect a new queue

Run the commands below from the repository with Node.js installed. Use a
scratch database rather than a production database when trying the examples.

```sh
DB="./ops-demo.sqlite"

npm run --silent queue -- enqueue --db "$DB" --queue reports \
  --kind export --key monthly-sales:2026-11 \
  --payload '{"report":"monthly-sales","month":"2026-11"}'

npm run --silent queue -- status --db "$DB" --queue reports
npm run --silent queue -- list --db "$DB" --queue reports --status queued
npm run --silent queue -- events --db "$DB" --queue reports
```

Each result is JSON. Use the ID from an enqueue or a list result to inspect a
single job and its history:

```sh
JOB='<paste job ID>'

npm run --silent queue -- get --db "$DB" --job "$JOB"
npm run --silent queue -- events --db "$DB" --job "$JOB" --limit 20
```

The CLI opens the supplied path as a normal queue client, even for inspection.
An incorrect filename can create an empty database. Confirm the target path
before treating zero jobs as a production signal.

## Backfill from a manifest

For a small one-time backfill, prepare a UTF-8 file with one JSON job per
line. Supply the destination queue once on the command line. An example
`backfill.ndjson` file:

```jsonl
{"kind":"export","payload":{"month":"2026-08"},"idempotencyKey":"export:2026-08"}
{"kind":"export","payload":{"month":"2026-09"},"idempotencyKey":"export:2026-09"}
{"kind":"export","payload":{"month":"2026-10"},"idempotencyKey":"export:2026-10"}
```

Import and verify the queue:

```sh
npm run --silent queue -- import --db "$DB" --queue reports --file ./backfill.ndjson
npm run --silent queue -- list --db "$DB" --queue reports --status queued
```

Each nonblank line is a single object. `kind` and `payload` are required.
The optional fields are `idempotencyKey`, `priority`, `availableAt`,
`maxAttempts`, and `retryPolicy`. Unknown fields are rejected instead of
silently ignored, so a misspelled flag cannot leave the wrong default in
place. Use an explicit key on any job that must not be inserted twice.

Files must be regular files up to 16 MiB and hold no more than 200 jobs. The
entire file is parsed before enqueueing; if a row is invalid or SQLite rejects
a write, none of that file's new jobs are inserted. The CLI cannot make
several separate imports one transaction. Chunk larger imports by a stable
partition and retain the return value to track which chunks committed.

The JSON result includes `inserted`, `reused`, and `results`. The `results`
array follows the input job order. It contains both newly inserted and reused
IDs so an operator can reconcile a retry. A manifest without any jobs returns
zero counts and an empty array.

### Backfill checklist

1. Confirm the database path, queue name, and handler kind before importing.
2. Put a stable business key on each intended replay-safe job. Use distinct
   keys when intentionally creating work for both an earlier and later period.
3. If the queue should not start yet, pause it or schedule a future
   `availableAt`. A pause applies to the whole queue, not just this manifest.
4. Compare inserted plus reused against the number of manifest rows. Investigate
   unexpected reuse instead of changing keys until the import succeeds.
5. Resume the queue if you paused it; then inspect the first completed jobs and
   any dead jobs before importing another batch.

## Diagnose availability

When work is not moving, ask these questions in order:

| Observation | Next Check |
| --- | --- |
| Queue is paused | Check whether a maintenance window is still active. |
| Only delayed work exists | Compare `nextAvailableAt` to worker time. |
| Ready count is growing | Check worker processes, handler configuration, and processing time. |
| Running count is steady | Check whether workers are making progress or waiting on dependencies. |
| Expired count is growing | Check worker liveness, lease sizing, and whether recovery is running. |
| Dead count is growing | Inspect the job history for a common failure mode. |
| Queue is empty unexpectedly | Confirm database path, selected queue, and producer health. |

An idle worker is not proof that a queue has no jobs. Jobs may be delayed,
the queue may be paused, or other workers may already hold its jobs. Use
status and event reads to distinguish these cases.

## Pause for maintenance

```sh
npm run --silent queue -- pause --db "$DB" --queue reports
npm run --silent queue -- status --db "$DB" --queue reports
npm run --silent queue -- resume --db "$DB" --queue reports
```

Pause prevents new claims. It does not stop existing handlers or block a
producer from inserting work. To quiesce a queue, pause it first, stop
producers if new arrivals are unwanted, and wait for active worker calls to
finish. Then inspect for expired attempts before resuming.

Use an abort signal to request a graceful shutdown of a poll loop. Await the
loop before closing the client; a running handler may still report a result.
For example:

```ts
const stop = new AbortController();
const onStop = () => stop.abort();
process.once("SIGTERM", onStop);

try {
  await poll(worker, { signal: stop.signal, idleDelayMs: 250 });
} finally {
  process.off("SIGTERM", onStop);
  client.close();
}
```

## Recover work

Explicit recovery is useful if there are no workers polling a queue. Use a
bounded sweep and run another one if more expired jobs remain:

```sh
npm run --silent queue -- reclaim --db "$DB" --queue reports --limit 50
npm run --silent queue -- list --db "$DB" --queue reports --status dead
```

Recovery does not prove that a handler stopped. Treat externally visible
work as at-least-once: a worker can write to an external system before it can
record its result in SQLite. Where possible, use the stable job ID or a
business identifier to make writes, emails, and follow-up submissions safe
to repeat. An enqueue idempotency key deduplicates submissions; it does not
make handler side effects exactly-once.

If a dead job failed because of an outside dependency, retry it after fixing
the dependency. Retrying a dead job reuses the same job ID:

```sh
npm run --silent queue -- retry --db "$DB" --job "$JOB" --max-attempts 2
```

If a job should no longer execute, cancel it instead. Cancellation changes
queue state, not the external side effects a handler has already performed.

## Handle bursts and contention

Write transactions are short and serialize writers. The transaction does not
stay open while a handler is awaiting application work. Keep job payloads
small; pass a stable reference when the underlying document is large.

For producers, batch related inserts when atomicity matters. A batch still
holds a writer lock until all of its inserts commit. For workers, tune lease
duration to typical processing time and arrange renewal for unusually long
tasks. Align the clock of all processes sharing the database.

If SQLite reports contention, check that the workload fits one local
database, that the file is not on a network share, and that you have not
created too many writer processes. A longer busy timeout may reduce immediate
errors but can also increase the time a caller waits. Don't turn a storage
exception into a successful processing outcome.

### Split workloads before increasing workers

Queues do not share pause settings, but workers using the same SQLite file
share its writer lock. Splitting `reports` from `deliveries` makes it easier
to tune poll intervals, pause just one stage, or give separate worker processes
their own handler sets. It does not isolate writes at the database level.

If one stage produces tasks for the next, decide what happens after a failure
between those two steps. A worker can enqueue follow-up jobs and then fail to
complete its own job. Make that handler safe to rerun by using stable keys for
the follow-up jobs. In the other direction, don't complete a parent attempt
before its required follow-up tasks have been durably accepted.

Priorities are local to each queue and order only jobs that are currently due.
A high-priority delayed job does not prevent ready work from running. A steady
stream of high-priority jobs can starve lower priorities; do not rely on a
priority value as a substitute for isolation or capacity planning.

### Choose a polling policy

An idle delay is a trade-off between response time and the frequency of polling
queries. Start with a delay comfortably below your required response time,
then measure idle wakeups and the age of queued work as load grows. Alerting
on a high count of delayed tasks can be misleading: scheduled work is expected
to accumulate before its due time. Track age of ready work separately.

A large batch bound reduces observer calls but can make each reported batch
contain outcomes spread over a longer window. The standard worker still runs
handlers sequentially. If you need parallelism, run independent workers and
give each process its own connection and worker name rather than sharing a
`QueueClient` among unrelated parts of the application.

## Retain event history deliberately

Inspect or export events before deleting them. The CLI expects an absolute
Unix millisecond threshold and deletes strictly older events:

```sh
CUTOFF=$(node -p 'Date.now() - 7 * 24 * 60 * 60 * 1000')
npm run --silent queue -- prune --db "$DB" --queue reports --before "$CUTOFF"
```

Pruning is irreversible without a backup. An event consumer should advance
its checkpoint only after successfully handling each event and should stay
within the retention window. Do not store credentials in payloads or failure
messages; both may be visible to operators, and failure messages can also
be copied to the journal.

## Test representative behavior

Use in-memory clients for validation and business rules, separate real
connections against a temporary file for persistence, and an injected clock
instead of sleeps for time-based checks. Use the same fake clock for all
clients in one scenario. Assert observable state and operation results rather
than assuming a particular statement order or SQL implementation.

The small example illustrates the API surface:

```sh
npm run example
```

The pipeline example uses a temporary on-disk database and cleans it up on
exit. It demonstrates producer replay, paused delivery, retries, and an event
reader that consumes a journal in pages:

```sh
npm run example:reports
```

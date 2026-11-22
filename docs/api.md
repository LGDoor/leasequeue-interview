# API reference

Import the library surface from `src/index.ts`. Source imports end in `.ts`;
the build rewrites those imports to `.js`. `QueueClient` methods are synchronous.
The buffered producer, worker, and polling helpers provide asynchronous operations.

## Connections

```ts
const client = openQueue("./reports.sqlite", {
  busyTimeoutMs: 5000,
  clock: { now: () => Date.now() },
});
```

| Operation | Behavior |
| --- | --- |
| `openQueue(filename, options?)` | Opens a client and migrates the database if needed. |
| `client.isClosed` | Indicates whether the client has been closed. |
| `client.close()` | Releases the SQLite handle. Calling it more than once is safe. |

Each `:memory:` client has its own database. Clients sharing a filename see the
same durable job state. Keep the connection open until its worker has stopped.
Closing a connection does not cancel its jobs or mark them finished.

`Clock.now()` is a nonnegative safe integer in Unix milliseconds. The default
clock uses wall time. In tests, inject a shared clock for precise scheduling.

## Producers

### `enqueue(input)`

Returns `{ jobId, inserted }`. A job starts queued and is not eligible for
claim before `availableAt`. Payloads are stored as JSON snapshots rather than
retaining references to the original objects.

| Input | Default | Valid Values |
| --- | --- | --- |
| `queue` | Required | Queue name, such as `nightly-reports` |
| `kind` | Required | Handler name, such as `export.csv` |
| `payload` | Required | JSON value, up to 64,000 encoded bytes |
| `idempotencyKey` | No deduplication | Nonblank string, up to 200 characters |
| `priority` | `0` | Integer from −10,000 to 10,000 |
| `availableAt` | Current time | Nonnegative Unix milliseconds |
| `maxAttempts` | `3` | Integer from 1 to 100 |
| `retryPolicy` | Default policy | Optional overrides described below |

Names start with a letter or digit and can contain letters, digits, dots,
underscores, and hyphens. Names are case-sensitive and at most 100 characters.
Idempotency keys are literal strings scoped to a queue. Repeating a key in
its queue returns the original ID with `inserted: false` and does not update
the original job. Keys remain associated with terminal jobs. Jobs submitted
without a key are always distinct.

### `enqueueMany(inputs)`

Accepts up to 200 jobs and returns one result per input in input order. The
batch is atomic: all new jobs are written together or none are. An empty
array returns `[]`. Duplicate inputs return existing IDs normally; they are
not errors. There is one `enqueued` event per inserted job.

```ts
const submitted = client.enqueueMany([
  { queue: "reports", kind: "export", payload: { month: "2026-10" } },
  { queue: "reports", kind: "export", payload: { month: "2026-11" } },
]);
```

### Buffered submissions

Use `new BufferedProducer(client, options?)` when independent callers send
bursts of jobs through one long-lived client. The producer combines calls
into bounded batches and writes each batch with `client.enqueueMany()`.
The existing synchronous client methods are still available.

```ts
const producer = new BufferedProducer(client, {
  batchSize: 32,
  maxPending: 256,
  lingerMs: 8,
});

const results = await Promise.all([
  producer.enqueue({ queue: "reports", kind: "export", payload: { region: "west" } }),
  producer.enqueue({ queue: "reports", kind: "export", payload: { region: "east" } }),
]);
```

`producer.enqueue(input)` validates and snapshots the input and returns a
promise. It resolves to the usual `{ jobId, inserted }` only after the write
succeeds. An explicit `availableAt` is kept; otherwise the job becomes
available as of the successful write. Callers must handle promise rejections.

| Option | Default | Meaning |
| --- | --- | --- |
| `batchSize` | `50` | Jobs per write transaction; 1–200 |
| `maxPending` | `1000` | Outstanding jobs, including the active batch; `batchSize`–10,000 |
| `lingerMs` | `10` | Milliseconds to wait for a partial batch; 0–60,000 |
| `retry.maxRetries` | `3` | Additional attempts on SQLite busy; 0–10 |
| `retry.initialDelayMs` | `10` | First retry delay in milliseconds; 0–60,000 |
| `retry.maxDelayMs` | `250` | Retry delay cap; at least the initial delay and at most 60,000 |

A full batch is scheduled immediately. A partial batch normally waits for
the linger window, rather than resetting that window on every new arrival.
Calls over the pending limit reject with `QueueError` code `producer-full`;
there is no automatic admission retry. Keep payloads small and handle
admission pressure in the producer's caller.

Only a SQLite busy response is retried. The same batch is attempted again
after a delay, which doubles up to the configured cap. Later batches do not
overtake it. If the writer fails permanently or runs out of retries, each
request in that batch rejects with the storage error. Later submissions can
still complete. The application must decide which failures to resubmit.

#### Flush, shutdown, and statistics

`flush()` expedites dispatch and waits for outstanding submissions accepted
before the call. It reports `{ accepted, inserted, reused, failed }` for
that group. Later submissions are not counted, even if they happen to be
written in the same transaction. The summary counts failures rather than
throwing them; the individual enqueue promises retain their errors.

`close()` stops accepting new work immediately, drains accepted work, and
returns the same flush summary. Repeated calls share the original close
promise. The producer doesn't close the underlying connection. Stop the
producer first, then close the client when its other users are also done.

`getStats()` returns a snapshot: `queued` and `inFlight` are current counts;
`accepted`, `inserted`, `reused`, `failed`, `dispatchedBatches`, and `retries`
are cumulative. A request rejected at admission is not `accepted` and does
not increment `failed`. `state` is `open`, `closing`, or `closed`.

#### Scheduling and durability

The default scheduler uses Node timers. Tests can supply a `schedule(delayMs,
run)` function that returns a cancellation function. The callback must be
deferred, including for a zero delay. This scheduler controls dispatch, not
the Unix clock used by the database for job timestamps.

The buffer is in memory. If the process exits before an enqueue promise
resolves, the caller should treat the outcome as uncertain. Use a stable
idempotency key for replay-safe jobs. SQLite writes still run synchronously
when a batch is dispatched; buffering reduces write transactions but does
not move SQLite work off the event loop.

## Job lifecycle

| State | Meaning | Normal exits |
| --- | --- | --- |
| `queued` | Waiting for its scheduled time or an available worker | `running`, `cancelled` |
| `running` | A worker holds a lease | `succeeded`, `queued`, `dead`, `cancelled` |
| `succeeded` | A worker completed the job | None |
| `dead` | Permanent failure or attempts exhausted | `queued` via operator retry |
| `cancelled` | Removed from processing by an operator | None |

`attempts` starts at zero and increments when a claim succeeds. Renewing a
lease, scheduling a retry, and looking up a job do not consume attempts. A
terminal job has a `finishedAt` timestamp and no lease.

### `claim({ queue, workerId, leaseMs? })`

Returns the next `ClaimedJob` or `null`. It considers only queued, available
jobs in the selected queue. It sorts by highest priority, then earliest
availability, then oldest insertion. A paused queue returns `null`.

The default lease is 30 seconds; leases can range from 1 millisecond to one
day. A lease is a time-limited ownership grant for that attempt. At the expiry
timestamp itself the lease has expired. Store the `leaseRef(job)` when making
calls on behalf of that attempt.

### `renew(lease, leaseMs)`

Extends the current, unexpired lease from the current time, never shortening
its previous expiry. Returns an updated `ClaimedJob`, or `null` when the
worker no longer owns the job. Use the returned snapshot if you need the new
expiry; the object from `claim()` is not mutated in place.

### `complete(lease)`

Marks a job succeeded and returns `true` when accepted. Returns `false`
when the operation cannot be applied to the current, unexpired lease.
A successful completion clears previous failure information.

### `fail(lease, options)`

Reports a failure while the worker still owns the lease. Supply a nonblank
`message`. Set `permanent: true` for errors that should not retry, or set
`retryAfterMs` to override the delay on a retryable failure. The outcome is
one of:

```ts
{ outcome: "retry", availableAt: 12000 }
{ outcome: "dead" }
{ outcome: "lost" }
```

A failure on the final attempt becomes dead even if an override delay was
provided. With the default policy the first retry waits 1,000 milliseconds,
and each subsequent delay doubles up to a 60,000 millisecond cap. Configure
`initialDelayMs`, `maxDelayMs`, and `multiplier` per job; the multiplier may
be fractional. A zero override schedules the retry immediately.

### `reclaimExpired(options?)`

Recovers expired running jobs, even in paused queues. Expired attempts with
budget remaining are queued immediately; otherwise they become dead. Returns
`{ requeued, dead }`, where both fields are job ID arrays. An optional `queue`
limits the sweep, and `limit` bounds the work (default 100, maximum 250).

A regular claim also recovers a bounded set of expired jobs in its own
unpaused queue. `get`, `list`, and `status` never recover work themselves.

### `cancel(jobId)` and `retryDead(jobId, options?)`

`cancel` changes a queued or running job to cancelled. It returns `false`
if nothing changed. Cancellation does not interrupt user code already
executing in a handler.

`retryDead` returns the updated job or `null` if it is missing or not dead.
It clears terminal metadata and resets attempts to zero. Supply `availableAt`
to delay the retry or `maxAttempts` to change the new budget. The payload and
idempotency association are preserved.

## Inspection

`get(jobId)` returns a `Job` snapshot or `null`. `list({ queue, status?, after?,
limit? })` returns `{ jobs, nextCursor }` in insertion order. Reuse the cursor
with the same filters and stop when it is `null`. The default page size is 50,
with a maximum of 250. Pages are live reads, not a snapshot across calls.

```ts
let after: number | undefined;
do {
  const page = client.list({ queue: "reports", status: "dead", after });
  for (const job of page.jobs) console.log(job.id, job.lastError);
  after = page.nextCursor ?? undefined;
} while (after !== undefined);
```

`status(queue)` returns counts by state, plus `ready` and `delayed` subsets
of queued jobs and the `expired` subset of running jobs. `nextAvailableAt`
is the earliest queued timestamp across ready and delayed jobs, or `null`.
Pausing does not change these counts. `getSettings(queue)` reports the pause
flag and its last change time; `setPaused(queue, paused)` changes that flag.
Repeating the same setting is a no-op.

## Event journal

`readEvents({ queue?, jobId?, after?, limit? })` returns events in sequence
order. `after` is exclusive. Set it to the largest sequence you consumed
to continue from there. A job filter omits queue-only events, and filters
combine when both are supplied. Use the sequence as a cursor even if times
are identical or the wall clock moves backwards.

Each event has `sequence`, `queue`, `jobId`, `name`, `occurredAt`, and
`details`. Events without an individual job have `jobId: null`. Job payloads
and lease tokens are not copied into events.

| Event | When emitted | Selected details |
| --- | --- | --- |
| `enqueued` | New insert | Kind, priority, availability |
| `claimed` | Successful claim | Worker ID, expiry, attempt |
| `lease-renewed` | Accepted renewal | Worker ID, expiry |
| `succeeded` | Accepted completion | Attempt |
| `retry-scheduled` | Retryable failure | Attempt, message, availability |
| `dead` | Terminal failure | Attempt, message |
| `lease-expired` | Recovery | Worker ID, previous expiry, attempts, resulting state |
| `cancelled` | Accepted cancellation | None |
| `retried` | Operator retry | Availability, new attempt budget |
| `queue-paused` / `queue-resumed` | Actual setting change | None |

`pruneEvents({ before, queue? })` deletes matching events strictly older than
`before` and returns the deleted count. It never modifies jobs. After pruning,
there can be sequence gaps; do not infer a missing event from a gap alone.

## Worker and poll loop

Construct `new Worker(client, { queue, workerId, leaseMs?, handlers })`.
Handler keys are job kinds. A callback receives `{ job, renew }` and can be
synchronous or asynchronous. Returning normally reports success. Throwing
`PermanentJobError` means no retry; throwing `RetryAfterError` supplies a
retry delay. Other handler errors use the job's retry policy. Missing handlers
are permanent failures. Errors from database operations can still propagate.

`runOnce()` reports `idle`, `succeeded`, `retry`, `dead`, or `lost`.
Non-idle results carry a `jobId`; retry results include `availableAt`.

`runBatch({ limit?, signal? })` makes at most `limit` attempts sequentially
(default 10, maximum 200). `drained: true` means a claim returned no job;
`false` includes stopping due to the bound or abort. An abort prevents the
next claim, not the completion of an active handler.

`poll(worker, { signal?, batchLimit?, idleDelayMs?, maxIterations?, onBatch?,
wait? })` repeats batches, waiting only after a batch observes no ready work.
An injected `wait` function can simulate time in tests. The result includes
the stop reason, number of completed batch iterations, number of reported
job outcomes, and number of iterations with no outcomes.

## Error handling

Use the `code` on a `QueueError` to distinguish an invalid input, a closed
connection, an unsupported schema, or internally inconsistent stored data.
Missing jobs and rejected state transitions normally return a sentinel
rather than throw. SQLite can throw for I/O, locking, or integrity errors;
don't interpret those exceptions as a missing job.

## Design notes

### Input and storage shapes

The client turns public input into validated values before calling storage.
An enqueue payload crosses the boundary as JSON text. Job rows use SQLite
column names; the codec constructs a fresh public snapshot when a row leaves
storage. Editing that snapshot does not edit the database.

`jobs.ordinal` is separate from the public job UUID. The ordinal provides
insertion order without committing the public interface to SQLite's internal
row identifiers. Job events get their own monotonically increasing sequence.
Avoid substituting either timestamp for those pagination values: multiple
writes can share a clock reading.

### Write and read boundaries

Operations such as claim, cancel, and fail update job state and insert their
journal records in one transaction. Observers should see either the entire
transition or none of it. Reads do not open long-lived transactions across API
calls, so an observer comparing a status summary to a later list should expect
that another client may have acted in between.

An immediate transaction reserves the single SQLite writer slot before
choosing a job to claim. Selecting a row and updating it in unrelated calls
would not give the same guarantee: another worker could act between them.
No transaction is held open while a handler awaits user work. An external
service call and a database transition are not one atomic operation.

### Time and attempts

The clock is sampled once at the client boundary for each timed operation. A
batch uses a common time for all its rows. This keeps a decision and its
journal entry consistent, and allows tests to set time exactly at a deadline.
Wall time is the default so durable deadlines can be compared across process
restarts. A fake clock is useful only when all relevant clients agree on it.

The database keeps the attempt budget on the job rather than the worker. If
the process exits, the next worker has the same information about previous
attempts. The active lease is separate from that budget. An expired final
attempt remains visible as running-but-expired until a claim or explicit
sweep processes it.

### Failure classification

The library separates application handler failures from database failures.
A handler can classify a failure as permanent or supply a retry delay. In
contrast, an exception from the storage layer should reach the worker host;
a queue failure is not evidence that the job's application payload was invalid.

Use `Worker` when a handler return or exception is enough to decide the job
outcome. Use the client methods directly when a process needs to coordinate
several steps outside the library. In either case, plan for processing to be
repeated after a lost worker or an uncertain external write.

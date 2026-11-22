import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { leaseRef } from "../src/index.ts";
import { claim, disk, input, memory } from "./helpers.ts";

describe("lease lifecycle", () => {
  test("completes a running job and removes its lease", (t) => {
    const { queue, clock } = memory(t, 1_000);
    const result = queue.enqueue(input());
    const leased = claim(queue, { leaseMs: 100 });

    assert.equal(leased.id, result.jobId);
    assert.equal(leased.attempts, 1);
    assert.equal(leased.updatedAt, 1_000);
    assert.deepEqual(leased.lease, {
      token: leased.lease.token,
      workerId: "worker-one",
      expiresAt: 1_100,
    });
    clock.advance(10);
    assert.equal(queue.complete(leaseRef(leased)), true);

    const finished = queue.get(leased.id);
    assert.ok(finished);
    assert.equal(finished.status, "succeeded");
    assert.equal(finished.lease, null);
    assert.equal(finished.finishedAt, 1_010);
    assert.equal(finished.updatedAt, 1_010);
    assert.equal(queue.complete(leaseRef(leased)), false);
  });

  test("does not complete an expired lease even before the next claim", (t) => {
    const { queue, clock } = memory(t);
    queue.enqueue(input());
    const leased = claim(queue, { leaseMs: 20 });
    clock.advance(20);

    assert.equal(queue.complete(leaseRef(leased)), false);
    assert.equal(queue.get(leased.id)?.status, "running");
    assert.deepEqual(queue.readEvents({ jobId: leased.id }).map(({ name }) => name), [
      "enqueued",
      "claimed",
    ]);
  });

  test("renews from the present time and never shortens a lease", (t) => {
    const { queue, clock } = memory(t, 1_000);
    queue.enqueue(input());
    const leased = claim(queue, { leaseMs: 100 });
    const ref = leaseRef(leased);
    clock.advance(20);

    const unchanged = queue.renew(ref, 5);
    assert.equal(unchanged?.lease.expiresAt, 1_100);
    const extended = queue.renew(ref, 150);
    assert.equal(extended?.lease.expiresAt, 1_170);
    assert.equal(extended?.lease.token, leased.lease.token);
    assert.equal(extended?.attempts, 1);

    clock.advance(100);
    assert.equal(queue.claim({ queue: "reports", workerId: "other-worker" }), null);
  });

  test("does not renew an expired lease", (t) => {
    const { queue, clock } = memory(t);
    queue.enqueue(input());
    const leased = claim(queue, { leaseMs: 25 });
    clock.advance(25);

    assert.equal(queue.renew(leaseRef(leased), 200), null);
    assert.equal(queue.get(leased.id)?.lease?.expiresAt, leased.lease.expiresAt);
  });

  test("allocates a new lease after recovery, including for the same worker name", (t) => {
    const fixture = disk(t, 100);
    const firstClient = fixture.open();
    const secondClient = fixture.open();
    const result = firstClient.enqueue(input({ maxAttempts: 3 }));
    const before = claim(firstClient, { workerId: "same-worker", leaseMs: 10 });
    fixture.clock.advance(10);

    const after = claim(secondClient, { workerId: "same-worker", leaseMs: 50 });
    assert.equal(after.id, result.jobId);
    assert.notEqual(before.lease.token, after.lease.token);
    assert.equal(after.lease.expiresAt, 160);
    assert.equal(after.attempts, 2);
  });

  test("a failure reported with an outdated token leaves the current lease alone", (t) => {
    const { queue, clock } = memory(t);
    queue.enqueue(input());
    const first = claim(queue, { leaseMs: 5 });
    clock.advance(5);
    const second = claim(queue, { workerId: "second-worker", leaseMs: 50 });

    assert.deepEqual(queue.fail(leaseRef(first), { message: "late failure" }), { outcome: "lost" });
    assert.equal(queue.get(second.id)?.lease?.token, second.lease.token);
    assert.equal(queue.get(second.id)?.status, "running");
  });
});

describe("recovery", () => {
  test("marks expired final attempts dead and requeues earlier attempts", (t) => {
    const { queue, clock } = memory(t, 1_000);
    const [finalAttempt, retryable] = queue.enqueueMany([
      input({ maxAttempts: 1 }),
      input({ maxAttempts: 2 }),
    ]);

    claim(queue, { leaseMs: 10 });
    claim(queue, { leaseMs: 10 });
    clock.advance(10);

    assert.deepEqual(queue.reclaimExpired(), {
      requeued: [retryable.jobId],
      dead: [finalAttempt.jobId],
    });

    const dead = queue.get(finalAttempt.jobId);
    const queued = queue.get(retryable.jobId);
    assert.ok(dead && queued);
    assert.equal(dead.status, "dead");
    assert.equal(dead.finishedAt, 1_010);
    assert.equal(dead.lease, null);
    assert.equal(queued.status, "queued");
    assert.equal(queued.attempts, 1);
    assert.equal(queued.lastError, "Lease expired");
    assert.equal(queued.availableAt, 1_010);
  });

  test("respects queue scope and limits while choosing oldest expiry first", (t) => {
    const { queue, clock } = memory(t);
    const [later, earlier, other] = queue.enqueueMany([
      input(),
      input(),
      input({ queue: "billing" }),
    ]);
    claim(queue, { leaseMs: 12 });
    claim(queue, { leaseMs: 6 });
    claim(queue, { queue: "billing", leaseMs: 3 });
    clock.advance(15);

    assert.deepEqual(queue.reclaimExpired({ queue: "reports", limit: 1 }), {
      requeued: [earlier.jobId],
      dead: [],
    });
    assert.equal(queue.get(other.jobId)?.status, "running");
    assert.deepEqual(queue.reclaimExpired(), {
      requeued: [other.jobId, later.jobId],
      dead: [],
    });
    assert.deepEqual(queue.reclaimExpired(), { requeued: [], dead: [] });
  });

  test("a claim only sweeps expired leases in its own queue", (t) => {
    const { queue, clock } = memory(t);
    const foreign = queue.enqueue(input({ queue: "billing" }));
    claim(queue, { queue: "billing", leaseMs: 5 });
    clock.advance(5);

    assert.equal(queue.claim({ queue: "reports", workerId: "worker-one" }), null);
    assert.equal(queue.get(foreign.jobId)?.status, "running");
  });
});

describe("job failures and retry", () => {
  test("applies backoff, respects the cap, and eventually exhausts attempts", (t) => {
    const { queue, clock } = memory(t, 1_000);
    const result = queue.enqueue(input({
      maxAttempts: 4,
      retryPolicy: { initialDelayMs: 10, maxDelayMs: 15, multiplier: 2 },
    }));

    for (const expectedDelay of [10, 15, 15]) {
      const job = claim(queue);
      const availableAt = clock.now() + expectedDelay;
      assert.deepEqual(queue.fail(leaseRef(job), { message: "Try later" }), {
        outcome: "retry",
        availableAt,
      });
      assert.equal(queue.get(job.id)?.lease, null);
      assert.equal(queue.claim({ queue: "reports", workerId: "worker-one" }), null);
      clock.advance(expectedDelay);
    }

    const final = claim(queue);
    assert.equal(final.id, result.jobId);
    assert.equal(final.attempts, 4);
    assert.deepEqual(queue.fail(leaseRef(final), { message: "Still unavailable" }), { outcome: "dead" });
    const saved = queue.get(final.id);
    assert.equal(saved?.finishedAt, clock.now());
    assert.equal(saved?.lastError, "Still unavailable");
  });

  test("uses a zero retry override without consuming an extra attempt", (t) => {
    const { queue, clock } = memory(t, 1_000);
    queue.enqueue(input());
    const first = claim(queue);

    assert.deepEqual(queue.fail(leaseRef(first), { message: "Immediate retry", retryAfterMs: 0 }), {
      outcome: "retry",
      availableAt: clock.now(),
    });
    assert.equal(queue.get(first.id)?.attempts, 1);
    assert.equal(claim(queue).attempts, 2);
  });

  test("manual retry resets the budget and can schedule a later run", (t) => {
    const { queue, clock } = memory(t, 1_000);
    const result = queue.enqueue(input({ maxAttempts: 1, idempotencyKey: "once" }));
    const first = claim(queue);
    assert.deepEqual(queue.fail(leaseRef(first), { message: "No space" }), { outcome: "dead" });
    clock.advance(5);

    const reset = queue.retryDead(result.jobId, { availableAt: 1_100, maxAttempts: 4 });
    assert.ok(reset);
    assert.equal(reset.status, "queued");
    assert.equal(reset.attempts, 0);
    assert.equal(reset.maxAttempts, 4);
    assert.equal(reset.lastError, null);
    assert.equal(reset.finishedAt, null);
    assert.equal(reset.idempotencyKey, "once");
    assert.equal(queue.retryDead(result.jobId), null);
    assert.equal(queue.claim({ queue: "reports", workerId: "worker-one" }), null);

    clock.set(1_100);
    assert.equal(claim(queue).attempts, 1);
  });

  test("a success after a retry clears the previous error", (t) => {
    const { queue } = memory(t);
    queue.enqueue(input());
    const first = claim(queue);
    queue.fail(leaseRef(first), { message: "Temporary", retryAfterMs: 0 });
    const second = claim(queue);
    assert.equal(second.lastError, "Temporary");
    assert.ok(queue.complete(leaseRef(second)));
    assert.equal(queue.get(first.id)?.lastError, null);
  });
});

describe("cancellation and pause", () => {
  test("cancellation is terminal for pending and running jobs", (t) => {
    const { queue, clock } = memory(t, 5_000);
    const [active, pending] = queue.enqueueMany([input(), input()]);
    const leased = claim(queue);
    clock.advance(5);

    assert.ok(queue.cancel(active.jobId));
    assert.ok(queue.cancel(pending.jobId));
    assert.equal(queue.cancel(active.jobId), false);
    assert.equal(queue.complete(leaseRef(leased)), false);
    assert.equal(queue.renew(leaseRef(leased), 50), null);
    assert.equal(queue.retryDead(active.jobId), null);
    assert.deepEqual(queue.fail(leaseRef(leased), { message: "Rejected" }), { outcome: "lost" });

    for (const jobId of [active.jobId, pending.jobId]) {
      const saved = queue.get(jobId);
      assert.equal(saved?.status, "cancelled");
      assert.equal(saved?.lease, null);
      assert.equal(saved?.finishedAt, 5_005);
    }
  });

  test("pausing prevents new claims without interrupting active work", (t) => {
    const { queue, clock } = memory(t, 2_000);
    const [active, waiting] = queue.enqueueMany([input(), input()]);
    const leased = claim(queue);
    assert.equal(leased.id, active.jobId);

    assert.deepEqual(queue.getSettings("reports"), {
      queue: "reports", paused: false, updatedAt: null,
    });
    assert.deepEqual(queue.setPaused("reports", true), {
      queue: "reports", paused: true, updatedAt: 2_000,
    });
    assert.ok(queue.complete(leaseRef(leased)));
    assert.equal(queue.claim({ queue: "reports", workerId: "worker-one" }), null);
    assert.equal(queue.status("reports").ready, 1);

    clock.advance(20);
    assert.equal(queue.setPaused("reports", true).updatedAt, 2_000);
    assert.equal(queue.setPaused("reports", false).updatedAt, 2_020);
    assert.equal(claim(queue).id, waiting.jobId);
  });
});

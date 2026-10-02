# WRITEUP

## 1. Where the atomic decision lives

Postgres makes every decision. One `READ COMMITTED` transaction runs these steps (`internal/store/store.go`, `reserveTx`):

1. **`pg_advisory_xact_lock(hashtextextended('user:'||user_id, 0))`.** This serialises all reserve attempts by *one user*, and different users never contend. The per-user limit and concurrent same-key retries depend on it. The lock is transaction-scoped, so a crash or rollback always releases it.
2. **Idempotency lookup** on `(user_id, idempotency_key)`, done under that lock.
3. **Per-user limit check.** We count the seats this user holds for the show. This is a read-then-decide, and it is safe *only because* of step 1: the only transactions that can raise this user's count are this user's own, and they run one at a time. A cancel can only lower the count.
4. **Seat row locks in a deterministic order:**
   `SELECT label, status FROM seats WHERE show_id=$1 AND label = ANY($2) ORDER BY label FOR UPDATE`.
   If any locked row is not `available`, we roll back and return `409 seat_taken`. Nothing is changed (all-or-nothing).
5. **Insert the reservation.** The row carries the idempotency key and a request fingerprint.
6. **Conditional update** guarded on current state:
   `UPDATE seats SET status='confirmed', reservation_id=…, user_id=… WHERE show_id=… AND label = ANY(…) AND status='available'`.
   We require `RowsAffected == len(seats)`, otherwise we roll back. While we hold the row locks this can't fail, but it is a second, independent guard. Then we commit.

**Why it's race-free.** The seat *row* is the seat: primary key `(show_id, label)`, with a single `reservation_id` column. A seat can't point at two reservations, by construction. A `CHECK` constraint ties `status` to whether there is an owner.

Take the 500-way race on A12. All 500 transactions queue on A12's row lock. The first one commits. Each waiter then wakes up, and under READ COMMITTED Postgres re-reads the *latest committed* row for `FOR UPDATE`. The waiter sees `confirmed` and declines. There is no window between "is it free?" and "take it", because the check happens on the locked row inside the transaction that writes it.

**Multi-seat and deadlock.** Every transaction locks the seats it wants in the same global order (`ORDER BY label`). So "T1 holds A12 and wants A13" while "T2 holds A13 and wants A12" can't happen. The advisory lock is always taken *before* any seat lock, so the lock order across all lock types is total. As a belt-and-braces measure, deadlock (`40P01`), serialization (`40001`), lock-timeout (`55P03`) and unique-violation (`23505`) errors restart the whole transaction, up to 8 times. `TestMultiSeatNoDeadlockAllOrNothing` fires overlapping pairs in opposite orders. It asserts zero 5xx, and checks that the confirmed count is always even (so no half-reservations).

**Fast path (an optimisation, not the decision).** Before opening the locking transaction, one read-only statement checks in a single snapshot whether this is a replay and whether any requested seat is already taken. After the winner of a hot seat commits, the other ~499 are declined by this read without touching locks: roughly 75–90% of burst declines in local runs (`reserve_fast_path_total`). It is safe for two reasons:

- A decline is truthful. The seat *was* taken at that snapshot.
- It can't misreport a retry as `seat_taken`. The reservation row and the seat update commit atomically, so any snapshot that shows the seat taken by our earlier attempt also shows that attempt's reservation row. It is one statement, so it is one snapshot.

The authoritative path is still the transaction above.

## 2. Idempotency

- **Where the key is stored:** in the `reservations` row itself (`idempotency_key`, `request_fingerprint`), under `UNIQUE (user_id, idempotency_key)`. The key and its result are written in the same transaction, so we can never have "key recorded, seats not taken" or the reverse. Keys are scoped per user, so one user can't collide with (or probe) another user's keys.
- **How exactly-once is enforced:** concurrent retries with one key serialise on the per-user advisory lock. The first one creates the reservation. Every later one finds the row in step 2 and gets **`200` with the original reservation** (and `Idempotent-Replayed: true`). The unique constraint is a backstop: if it ever fired, the retry loop would re-run and replay. A replay returns `200` rather than `201`, so a stormed seat sees exactly one `201` even when the winner retries.
- **Same key, different body:** the fingerprint is `show_id | sorted(seats)`, so seat order doesn't matter. If it differs we return `409 idempotency_key_conflict` with `original_reservation_id`, and nothing moves.
- **Failed attempts are not stored.** A `409 seat_taken` leaves no row, so retrying that key re-evaluates. The key reserves "at most once successfully". It does not freeze a failure.
- **No key supplied:** we generate a random one. The request works, but it isn't retry-safe. That's the client's choice.
- **Replay after a cancel** returns the original (now `cancelled`) reservation. It does not re-book.

## 3. Holds and expiry (chosen model)

I chose **reserve = confirm immediately, plus an explicit owner-only cancel** (`POST /reservations/{id}/cancel`). The spec's `201` body is `"status":"confirmed"`, and this model has no background timers to reason about under load.

- **Only the owner can cancel.** We lock the reservation row `FOR UPDATE` and compare its `user_id` with the token's subject. Others get `403`, and anonymous callers get `401`.
- **A release can never resurrect someone else's seat.** The release is `UPDATE seats SET status='available' … WHERE reservation_id = $this`, so it can only touch seats that still point at *this* reservation. A second or stale cancel is a no-op `200`. The burst script checks this live: it cancels, re-books with another user, re-cancels, and confirms the seat is still sold.
- **The released seat is cleanly bookable**, because it goes back to exactly the `available` state, with no owner, that the reserve path guards on.

The schema already allows `status='held'`, and every count reports `held`. Adding time-boxed holds would mean:

- reserve writes `held` with a `held_until`
- `POST /reservations/{id}/confirm` turns `held` into `confirmed`, guarded on `status='held' AND held_until > now()`
- expiry is a sweeper running `UPDATE … SET available WHERE status='held' AND held_until < now() RETURNING …`, in small batches with `FOR UPDATE SKIP LOCKED`
- the reserve path also treats an expired hold as available, inside the same locked check, so correctness doesn't depend on how often the sweeper runs

## 4. Consistency vs availability under a partition

This is a CP system on purpose. Postgres is the single source of truth, and if the app can't reach it we **refuse rather than guess**:

- `/readyz` returns `503` (it fails closed), so the load balancer stops routing to the instance
- reserve returns `503 unavailable` with `Retry-After`. That is safe because of idempotency keys.

Overselling a seat is the one unrecoverable failure; turning a buyer away for a few seconds isn't. Reads (`GET /shows/{id}`) could be served stale from a replica or cache during a partition (AP for reads), but writes must never be. App instances are stateless and the cache holds only immutable show metadata, so running N instances is safe. All coordination is in the database.

## 5. Observability: what pages me at 2am

Alerts, in priority order:

1. **`min(seat_invariant_ok) == 0`, or `/shows/{id}/audit` returns `ok:false`**, which logs `AUDIT FAILED` at error level. This means the data is corrupt. Page immediately.
2. **Any 5xx:** `sum(rate(http_requests_total{code=~"5.."}[5m])) > 0` or `reserve_internal_errors_total` increasing. Declines are 4xx by design, so any 5xx is a real fault, usually the DB.
3. **`/readyz` failing** on more than one probe.
4. **DB pool saturation:** `db_pool_acquired_conns == db_pool_max_conns` for minutes, together with p99 of `http_request_duration_seconds{route="POST /shows/{id}/reserve"}` above the SLO. This means we need more capacity, or a lock is stuck.

Dashboard only, not a page: the decline mix by reason (a spike in `idempotency_conflict` usually means a buggy client), and the fast-path ratio.

Every log line carries a `request_id`, so any client complaint can be traced from one header to the exact decision and outcome. The metrics reconcile with the API: the seat gauges are queried from the DB at scrape time, not kept in memory, and the burst script asserts that the counters equal what the client observed.

## 6. Load results (local, 4 vCPU; app, Postgres and the client all on one box)

- 20,000 reserve requests released at once, at concurrency 20,000.
- Results: **0 × 5xx, 0 network errors**, exactly one `201` per hot seat, every user at exactly 4/4, the invariant held in every poll, and the audit came back `ok`.
- About 2.5–4k req/s, limited by CPU on the shared box. Under a full 20k-at-once burst, p50 latency is mostly time spent queueing for the 40 DB connections. At concurrency 300 the same 20k requests finished at p99 ≈ 390 ms.
- I tried an in-process lock-striping "gate" in front of the transaction, to stop waiters from holding pooled connections. It made things worse locally (by serialising the fast path), so I removed it. That is recorded in the commit history.

## 7. AI usage (directed vs decided)

This service was built with **Claude Code (an AI coding agent)**, working from the assignment text I gave it.

- **Generated by the AI:** the code, tests, burst client, Docker/deploy files and the first draft of this write-up. It ran the integration tests and 20k-request bursts against local Postgres and Docker, and it iterated when something failed:
  - the first rerun of the tests surfaced per-user key collisions, which were fixed in the tests (the service behaviour was correct)
  - the first burst plan sold out before the per-user-limit scenario could bite
  - the lock-striping experiment was slower, so it was reverted
- **Directed and decided by me:**

  > **Candidate: replace this paragraph with your own account.** Cover which choices you reviewed and why you agree with them, or what you changed, and how you deployed and checked the live URL. Expect to be asked to extend this live.

- **Decisions worth being able to defend (they are the core of the design):**
  - row locks in `ORDER BY label` order, plus a conditional update, rather than SERIALIZABLE
  - a per-user advisory lock for the limit and for key races
  - the idempotency key in the same row and transaction as the reservation
  - replay returns `200`, not `201`
  - all-or-nothing partial requests
  - DB-backed gauges
  - CP under partition

## 8. What I'd do next

- **Time-boxed holds** plus `/confirm` and a `SKIP LOCKED` sweeper (section 3), with a `holds_expired_total` metric.
- **Real authentication** (OIDC/JWKS) instead of the `/auth/token` test mint, and an admin role inside the JWT.
- **Hot-show scaling.** The fast-path read could move to a replica, or a per-seat Redis `SETNX` gate could shed hot-seat losers before they reach Postgres, with Postgres staying the source of truth. Use PgBouncer in transaction mode to raise the connection ceiling.
- **Idempotency key TTL and cleanup**, plus a payment step: an outbox for the charge, keyed by `reservation_id`, so a charge is also exactly-once.
- **Tracing** (OpenTelemetry) with `request_id` as a span attribute, and a Grafana dashboard checked into the repo.
- **Property-based or Jepsen-style tests** that inject DB failovers in the middle of a burst.

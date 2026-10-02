# Seat Reservation at Scale

A small Java 21 / Spring Boot 3 + PostgreSQL service that sells assigned seats for a show. It never sells a seat twice, never lets a user go over their per-show limit, and never double-books a retried request, even when tens of thousands of buyers hit the same seats in the same second.

- **Live URL:** https://seat-reservation-rpn6.onrender.com  (free Render instance: the first request after ~15 min idle takes up to a minute to wake; `/readyz` shows when it is up)
- **Design and trade-offs:** [WRITEUP.md](WRITEUP.md)

## Quick start (clean checkout)

One command builds and starts everything, waits until it's healthy, smoke-tests the API, and opens it in your browser:

```bash
./run-local.sh            # macOS / Linux / Git Bash   (add --burst to also run the 20k burst)
.\run-local.ps1           # Windows PowerShell
```

Or step by step:

```bash
docker compose up --build -d          # app on :8080 + postgres
curl localhost:8080/readyz            # {"db":"ok","status":"ready"}
./burst.sh http://localhost:8080      # 20k-request stampede + verification
```

The defaults in compose are `ADMIN_TOKEN=dev-admin-token` and a dev `JWT_SECRET`. Override them with env vars.

Without Docker (needs JDK 21, Maven and a Postgres):

```bash
mvn -B -DskipTests package
DATABASE_URL=postgres://postgres:postgres@localhost:5432/seats?sslmode=disable java -jar target/seat-reservation.jar
make test        # integration tests against TEST_DATABASE_URL
```

## One-command burst

```bash
ADMIN_TOKEN=<admin token> ./burst.sh <BASE_URL>          # or: make burst BASE_URL=...
./burst.sh <BASE_URL> -requests 20000 -concurrency 5000  # tune the load
java burst/Burst.java -url <BASE_URL>                    # same thing, directly
```

The burst client is one Java file that uses only the JDK, so there is no build step. `burst.sh` runs it with your local Java 21+, or in an `eclipse-temurin:21-jdk` container if you don't have one.

It creates a fresh 2,500-seat show (limit 4) and fetches tokens for about 8k users. It then releases 20,000 reserve requests at once:

| scenario | what it fires | must hold |
|---|---|---|
| hot-seat storm | 5 hot seats × 500 distinct users | exactly one `201` per seat, 499 × `409 seat_taken` |
| idempotent retry | 1,000 users × 3 copies, same key and body | one reservation per key; copies get `200` with the same `reservation_id` |
| same key, different body | 200 users × 2 requests, same key, different seats | never two reservations; the loser gets `409 idempotency_key_conflict` |
| per-user limit | 100 users × 10 parallel reserves of private seats | exactly 4 confirmed per user, 6 × `409 per_user_limit_exceeded` |
| spoofed identity | 100 requests with `"user_id":"victim-user"` in the body | booked as the token's user |
| crowd | the rest, 1–3 random seats each | no 5xx |

While the burst runs, it polls `GET /shows/{id}` and checks `available + held + confirmed == total_seats` (it also recounts the per-seat list). After the burst it:

- cancels one winner and checks that a non-owner cancel gets `403`, the seat can be booked again, and a second (stale) cancel doesn't free it again
- calls `GET /shows/{id}/audit`
- scrapes `/metrics` and checks the counters against what the client saw

It prints the outcome distribution, latency percentiles and a PASS/FAIL table, and exits non-zero if any check fails. Sample output from a local run:

```
  [PASS] hot seat A12: exactly one 201                           201=1 409=499 other=0
  ...
  [PASS] zero 5xx across the burst                               5xx=0 network_errors=0
  [PASS] invariant holds after burst                             available=605 held=0 confirmed=1895 total=2500
  [PASS] per-user limit: 10 parallel reserves -> exactly limit confirmed 100 users; 201s per user min=4 max=4 (limit 4)
  [PASS] metric reservations_confirmed_total == observed 201s    metric=1487 observed=1487
  ...
all 20 checks passed
```

20k concurrent sockets need about 20k file descriptors on the client machine. `burst.sh` raises `ulimit -n` when it can. Otherwise, lower `-concurrency`.

## API

All bodies are JSON (any `Content-Type` is accepted). Money is integer paise: `250.5` and `"250"` are rejected with `400`.

| method & path | auth | notes |
|---|---|---|
| `POST /auth/token` `{"user_id":"alice"}` | none | returns an HS256 JWT (24h). This stands in for a real IdP so testers can mint users. |
| `POST /shows` `{"name","seats":[...],"price_paise",["per_user_limit"]}` | `Authorization: Bearer <ADMIN_TOKEN>` | `201`, the show with every seat `available`. `per_user_limit` defaults to 4. |
| `GET /shows/{id}` | none | per-seat status and `counts {available, held, confirmed, total_seats}` |
| `GET /shows/{id}/audit` | none | cross-checks seats against reservations in one snapshot. `ok:true` means consistent. |
| `POST /shows/{id}/reserve` `{"seats":["A12"],"idempotency_key":"..."}` | user JWT | see below |
| `GET /reservations/{id}` | owner JWT | |
| `POST /reservations/{id}/cancel` | owner JWT | `200`, the cancelled reservation (repeat cancels are no-ops), `403` if not the owner |
| `GET /healthz` | none | liveness: the process is up |
| `GET /readyz` | none | readiness: migrated and the DB answers within about 1s, else `503` |
| `GET /metrics` | none | Prometheus |

The idempotency key can go in the `Idempotency-Key` header or in the body (if both are sent they must match). Keys are scoped per user.

### Reserve outcomes

| status | `error` | meaning |
|---|---|---|
| `201` | – | `{"reservation_id","show_id","user_id","seats","amount_paise","status":"confirmed"}` |
| `200` | – | idempotent replay: the original reservation, plus header `Idempotent-Replayed: true` |
| `409` | `seat_taken` | at least one seat is held or sold. **Nothing** was reserved (all-or-nothing). |
| `409` | `per_user_limit_exceeded` | the reservation would take the user over `per_user_limit` for this show |
| `409` | `idempotency_key_conflict` | the key was already used with a different show or seat set |
| `400` | `unknown_seats` / `invalid_*` | validation error |
| `401` / `404` | | missing or bad token / no such show |
| `503` | `unavailable` | only when the database is genuinely unreachable; safe to retry with the same key |

**Partial requests are all-or-nothing.** If you ask for `["A12","A13"]` and A13 is taken, you get `409 seat_taken` listing A13, and A12 stays available.

## Observability

- **Metrics** (`/metrics`):
  - `reservations_confirmed_total{show_id}`
  - `reservations_declined_total{show_id,reason}`, with reason one of `seat_taken|per_user_limit|idempotent_replay|idempotency_conflict|unknown_seat`
  - `seats_available{show_id}`, `seats{show_id,status}`, `show_total_seats{show_id}` and `seat_invariant_ok{show_id}`. These are read from the database at scrape time, so they always match `GET /shows/{id}`.
  - also: `seats_confirmed_total`, `seats_released_total`, `reservations_cancelled_total`, `reserve_fast_path_total`, `reserve_internal_errors_total`, `http_requests_total{route,code}`, `http_request_duration_seconds`, `http_requests_in_flight`
  - DB pool: `db_pool_acquired_conns`, `db_pool_idle_conns`, `db_pool_pending_acquires`, `db_pool_max_conns`
  - standard `jvm_*` metrics
- **Logs:** one JSON object per line on stdout (Spring Boot structured logging, logstash format). Every line carries `request_id` (taken from `X-Request-ID` or generated, and echoed back in the response header). The access line also has `route`, `status`, `duration_ms`, `user_id` and `outcome`. Confirmations and cancels get their own lines with `reservation_id` and `seats`. Read them with `docker compose logs -f app`, or in the platform's log viewer.

Useful queries:

```promql
sum by (reason) (rate(reservations_declined_total[1m]))
sum(rate(http_requests_total{code=~"5.."}[5m]))
min(seat_invariant_ok)                         # must always be 1
```

## Deploy

**Render (blueprint in `render.yaml`):** in Render, choose New → Blueprint and select this repo. This creates a Postgres and the Docker web service, and wires up `DATABASE_URL` and `JWT_SECRET`. Set `ADMIN_TOKEN` when prompted. The health check is `/readyz`.

**Fly.io:** see the comments in `fly.toml`. It is set to 1 GB.

**Railway / anything else that runs a Dockerfile:** set `DATABASE_URL`, `JWT_SECRET` and `ADMIN_TOKEN`. Both `postgres://user:pass@host/db` and `jdbc:postgresql://...` forms are accepted.

**Memory:** under a full 20k-connection burst the JVM peaks at about 470–500 MB RSS. It passed every check inside a hard 512 MiB container limit, but that is tight, so use a 1 GB instance where you can. Free instances that sleep when idle wake on the first request: the app starts listening at once and migrates with retry/backoff, and the burst script waits for `/readyz`.

| env | default | |
|---|---|---|
| `PORT` | `8080` | |
| `DATABASE_URL` | local postgres | |
| `DB_MAX_CONNS` | `40` | keep this under the database's `max_connections` |
| `HTTP_THREADS` | `256` | worker threads; caps per-request memory |
| `JAVA_OPTS` | see Dockerfile | heap = 50% of the container limit |
| `JWT_SECRET` | insecure dev value (logs a warning) | |
| `ADMIN_TOKEN` | `dev-admin-token` (logs a warning) | |
| `LOG_LEVEL` | `info` | |

## Layout

```
src/main/java/com/paytm/seats/
  store/SeatStore.java    every correctness decision (JDBC transactions) + audit
  store/Models.java       records: Show, Reservation, ReserveResult, Audit, ...
  api/                    controllers, request-id/log/metrics filter, JSON + error mapping
  auth/Authenticator.java JWT issue/verify, admin token
  metrics/Metrics.java    Prometheus registry, DB-backed seat gauges, pool gauges
  config/                 Hikari pools (main + health), cold-start migrator, Tomcat tuning
src/main/resources/       application.yml, schema.sql
src/test/java/...         integration tests against real Postgres
burst/Burst.java          stampede + verification client (single file, JDK only)
```

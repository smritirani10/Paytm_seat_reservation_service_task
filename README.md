# Seat Reservation at Scale

A small Go + PostgreSQL service that sells assigned seats for a show. It never sells a seat twice, never lets a user go over their per-show limit, and never double-books a retried request, even when tens of thousands of buyers hit the same seats in the same second.

- **Live URL:** `<add your deployed URL here>` (see [Deploy](#deploy))
- **Design and trade-offs:** [WRITEUP.md](WRITEUP.md)

## Quick start (clean checkout)

```bash
docker compose up --build -d          # app on :8080 + postgres
curl localhost:8080/readyz            # {"db":"ok","status":"ready"}
./burst.sh http://localhost:8080      # 20k-request stampede + verification
```

The defaults in compose are `ADMIN_TOKEN=dev-admin-token` and a dev `JWT_SECRET`. Override them with env vars.

Without Docker (needs Go 1.25+ and a Postgres):

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/seats?sslmode=disable go run ./cmd/server
make test        # integration tests against TEST_DATABASE_URL
```

## One-command burst

```bash
ADMIN_TOKEN=<admin token> ./burst.sh <BASE_URL>          # or: make burst BASE_URL=...
./burst.sh <BASE_URL> -requests 20000 -concurrency 5000  # tune the load
```

The burst creates a fresh 2,500-seat show (limit 4) and fetches tokens for about 8k users. It then releases 20,000 reserve requests at once:

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
  [PASS] invariant holds after burst                             available=610 held=0 confirmed=1890 total=2500
  [PASS] per-user limit: 10 parallel reserves -> exactly limit confirmed 100 users; 201s per user min=4 max=4 (limit 4)
  [PASS] metric reservations_confirmed_total == observed 201s    metric=1513 observed=1513
  ...
all 20 checks passed
```

20k concurrent sockets need about 20k file descriptors on the client machine. `burst.sh` raises `ulimit -n` when it can. Otherwise, lower `-concurrency`.

## API

All bodies are JSON. Money is integer paise; a float `price_paise` is rejected with `400`.

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
| `GET /readyz` | none | readiness: migrated and the DB answers within 1s, else `503` |
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
  - `seats_available{show_id}`, `seats{show_id,status}` and `seat_invariant_ok{show_id}`. These are read from the database at scrape time, so they always match `GET /shows/{id}`.
  - also: `seats_confirmed_total`, `seats_released_total`, `reservations_cancelled_total`, `reserve_fast_path_total`, `reserve_internal_errors_total`, `http_requests_total{route,code}`, `http_request_duration_seconds`, `db_pool_*`
- **Logs:** one JSON line per request on stdout, with `request_id` (taken from `X-Request-ID` or generated, and echoed back in the response header), `route`, `status`, `duration_ms`, `user_id` and `outcome`. Confirmations and cancels get their own lines with `reservation_id` and `seats`. Read them with `docker compose logs -f app`, or in the platform's log viewer.

Useful queries:

```promql
sum by (reason) (rate(reservations_declined_total[1m]))
sum(rate(http_requests_total{code=~"5.."}[5m]))
min(seat_invariant_ok)                         # must always be 1
```

## Deploy

**Render (blueprint in `render.yaml`):** in Render, choose New → Blueprint and select this repo. This creates a free Postgres and the Docker web service, and wires up `DATABASE_URL` and `JWT_SECRET`. Set `ADMIN_TOKEN` when prompted. The health check is `/readyz`. Free instances sleep when idle: the first request wakes them, the app starts listening at once, and it runs migrations with retry/backoff. The burst script waits for `/readyz` before it starts.

**Fly.io:** see the comments in `fly.toml`.

**Railway / anything else that runs a Dockerfile:** set `DATABASE_URL`, `JWT_SECRET` and `ADMIN_TOKEN`.

| env | default | |
|---|---|---|
| `PORT` | `8080` | |
| `DATABASE_URL` | local postgres | |
| `DB_MAX_CONNS` | `40` | keep this under the database's `max_connections` |
| `JWT_SECRET` | insecure dev value (logs a warning) | |
| `ADMIN_TOKEN` | `dev-admin-token` (logs a warning) | |
| `LOG_LEVEL` | `info` | |

## Layout

```
cmd/server        main: config, pool, cold-start migration loop, graceful shutdown
cmd/burst         stampede + verification client
internal/store    schema.sql and every correctness decision (transactions)
internal/api      HTTP handlers, request-id/log/metrics middleware, integration tests
internal/auth     JWT issue/verify, admin token
internal/metrics  Prometheus registry, DB-backed seat gauges
```

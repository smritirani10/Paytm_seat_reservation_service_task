#!/usr/bin/env bash
# One command: build + start (app + Postgres), wait until healthy, smoke-test
# the API end to end, then open it in the browser.
#   ./run-local.sh            # start and open
#   ./run-local.sh --burst    # also run the 20k-request burst
set -euo pipefail
cd "$(dirname "$0")"
URL="http://localhost:8080"
ADMIN_TOKEN="${ADMIN_TOKEN:-dev-admin-token}"

command -v docker >/dev/null || { echo "Install Docker Desktop first: https://www.docker.com/products/docker-desktop/"; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker is installed but not running - start Docker Desktop and retry."; exit 1; }

echo "==> building and starting (first run downloads images, ~2-4 min)"
ADMIN_TOKEN="$ADMIN_TOKEN" docker compose up --build -d

echo -n "==> waiting for $URL/readyz "
for i in $(seq 1 120); do
  if curl -fs "$URL/readyz" >/dev/null 2>&1; then echo " ready"; break; fi
  echo -n "."; sleep 2
  [ "$i" = 120 ] && { echo; echo "not ready - logs:"; docker compose logs --tail 50 app; exit 1; }
done

echo "==> smoke test"
json() { python3 -c "import sys,json;print(json.load(sys.stdin)['$1'])" 2>/dev/null || sed -E "s/.*\"$1\":\"([^\"]+)\".*/\1/"; }
SHOW=$(curl -fs -X POST "$URL/shows" -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name":"friday-night","seats":["A1","A2","A12","A13"],"price_paise":25000}' | json id)
TOK_A=$(curl -fs -X POST "$URL/auth/token" -d '{"user_id":"alice"}' | json token)
TOK_B=$(curl -fs -X POST "$URL/auth/token" -d '{"user_id":"bob"}' | json token)
A=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/shows/$SHOW/reserve" -H "Authorization: Bearer $TOK_A" -d '{"seats":["A12"],"idempotency_key":"smoke-1"}')
R=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/shows/$SHOW/reserve" -H "Authorization: Bearer $TOK_A" -d '{"seats":["A12"],"idempotency_key":"smoke-1"}')
B=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/shows/$SHOW/reserve" -H "Authorization: Bearer $TOK_B" -d '{"seats":["A12"],"idempotency_key":"smoke-2"}')
echo "   alice reserves A12 -> $A (want 201)"
echo "   alice retries same key -> $R (want 200 replay)"
echo "   bob wants A12 -> $B (want 409 seat_taken)"
[ "$A$R$B" = "201200409" ] && echo "   smoke test PASSED" || echo "   smoke test FAILED"

if [ "${1:-}" = "--burst" ]; then ADMIN_TOKEN="$ADMIN_TOKEN" ./burst.sh "$URL"; fi

echo
echo "Running at $URL   (show state: $URL/shows/$SHOW   metrics: $URL/metrics)"
echo "Logs: docker compose logs -f app      Stop: docker compose down"
for open in open xdg-open; do command -v $open >/dev/null && { $open "$URL/shows/$SHOW" >/dev/null 2>&1 & break; }; done

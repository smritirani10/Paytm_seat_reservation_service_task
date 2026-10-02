#!/usr/bin/env bash
# One-command on-sale stampede + verification.
#   ./burst.sh <BASE_URL> [extra flags, e.g. -requests 20000 -concurrency 5000]
# Needs ADMIN_TOKEN in the environment (to create a fresh show).
# Exit code is 0 only if every correctness check passed.
set -euo pipefail
cd "$(dirname "$0")"

BASE_URL="${1:-${BASE_URL:-http://localhost:8080}}"
[ $# -gt 0 ] && shift
export ADMIN_TOKEN="${ADMIN_TOKEN:-dev-admin-token}"

# 20k concurrent sockets need file descriptors; best effort.
ulimit -n 65535 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

if command -v java >/dev/null 2>&1 && java -version 2>&1 | grep -qE 'version "(2[1-9]|[3-9][0-9])'; then
  exec java burst/Burst.java -url "$BASE_URL" "$@"
fi
echo "Java 21+ not found; running the burst client in a JDK container" >&2
exec docker run --rm --network host -e ADMIN_TOKEN --ulimit nofile=65535:65535 \
  -v "$PWD/burst:/burst:ro" eclipse-temurin:21-jdk java /burst/Burst.java -url "$BASE_URL" "$@"

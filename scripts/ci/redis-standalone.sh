#!/usr/bin/env bash
set -euo pipefail

readonly image="redis:7.4-alpine@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99"
readonly name="${REDIS_STANDALONE_NAME:-gobullmq-redis-standalone}"
readonly port="${REDIS_STANDALONE_PORT:-6390}"

up() {
  down
  docker run --detach --rm \
    --name "$name" \
    --publish "$port:6379" \
    "$image" \
    redis-server --maxmemory-policy noeviction >/dev/null

  for _ in $(seq 1 60); do
    if docker exec "$name" redis-cli ping 2>/dev/null | grep -q PONG; then
      echo "Redis standalone is ready on 127.0.0.1:$port"
      return
    fi
    sleep 1
  done
  logs
  echo "Redis standalone did not become ready" >&2
  return 1
}

down() {
  docker rm --force "$name" >/dev/null 2>&1 || true
}

logs() {
  docker logs "$name" 2>&1 || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  logs) logs ;;
  *) echo "usage: $0 <up|down|logs>" >&2; exit 2 ;;
esac

#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
readonly root
readonly standalone="$root/scripts/ci/redis-standalone.sh"
readonly cluster="$root/scripts/ci/redis-cluster.sh"
current=""

cleanup() {
  local status=$?
  if [[ "$status" -ne 0 ]]; then
    if [[ "$current" == "standalone" ]]; then
      "$standalone" logs
    elif [[ "$current" == "cluster" ]]; then
      "$cluster" logs
    fi
  fi
  "$standalone" down
  "$cluster" down
  return "$status"
}
trap cleanup EXIT

cd "$root/integration/node"
npm ci --ignore-scripts --no-audit
cd "$root"

current="standalone"
"$standalone" up
REDIS_MODE=standalone \
REDIS_ADDRS=127.0.0.1:6390 \
go test -tags=interop -count=1 -v ./integration/interop
"$standalone" down

current="cluster"
"$cluster" up
REDIS_MODE=cluster \
REDIS_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005 \
go test -tags=interop -count=1 -v ./integration/interop
"$cluster" down
current=""

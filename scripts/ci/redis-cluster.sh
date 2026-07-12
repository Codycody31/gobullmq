#!/usr/bin/env bash
set -euo pipefail

readonly image="redis:7.4-alpine@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99"
readonly prefix="${REDIS_CLUSTER_PREFIX:-gobullmq-redis-cluster}"
readonly first_port=7000
readonly last_port=7005

container_name() {
  echo "${prefix}-$1"
}

down() {
  local port
  for port in $(seq "$first_port" "$last_port"); do
    docker rm --force "$(container_name "$port")" >/dev/null 2>&1 || true
  done
}

logs() {
  local port name
  echo "CLUSTER INFO"
  docker exec "$(container_name "$first_port")" redis-cli -p "$first_port" CLUSTER INFO 2>&1 || true
  echo "CLUSTER NODES"
  docker exec "$(container_name "$first_port")" redis-cli -p "$first_port" CLUSTER NODES 2>&1 || true
  for port in $(seq "$first_port" "$last_port"); do
    name="$(container_name "$port")"
    echo "container log: $name"
    docker logs "$name" 2>&1 || true
  done
}

up() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    echo "Redis Cluster provisioning requires Linux host networking" >&2
    return 1
  fi

  down
  trap 'logs; down' ERR INT TERM

  local port name
  for port in $(seq "$first_port" "$last_port"); do
    name="$(container_name "$port")"
    docker run --detach --rm \
      --name "$name" \
      --network host \
      "$image" \
      redis-server \
      --port "$port" \
      --bind 127.0.0.1 \
      --protected-mode no \
      --appendonly no \
      --cluster-enabled yes \
      --cluster-config-file nodes.conf \
      --cluster-node-timeout 5000 \
      --cluster-announce-ip 127.0.0.1 \
      --cluster-announce-port "$port" \
      --cluster-announce-bus-port "$((port + 10000))" \
      --maxmemory-policy noeviction >/dev/null
  done

  for port in $(seq "$first_port" "$last_port"); do
    name="$(container_name "$port")"
    for _ in $(seq 1 60); do
      if docker exec "$name" redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; then
        break
      fi
      sleep 1
    done
    if ! docker exec "$name" redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; then
      echo "Redis node on port $port did not become ready" >&2
      return 1
    fi
  done

  local addresses=()
  for port in $(seq "$first_port" "$last_port"); do
    addresses+=("127.0.0.1:$port")
  done
  docker exec "$(container_name "$first_port")" redis-cli \
    --cluster create "${addresses[@]}" \
    --cluster-replicas 1 \
    --cluster-yes >/dev/null

  for _ in $(seq 1 60); do
    if docker exec "$(container_name "$first_port")" redis-cli -p "$first_port" CLUSTER INFO 2>/dev/null | grep -q '^cluster_state:ok'; then
      trap - ERR INT TERM
      echo "Redis Cluster is ready on ports $first_port-$last_port"
      return
    fi
    sleep 1
  done

  echo "Redis Cluster did not reach cluster_state:ok" >&2
  return 1
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  logs) logs ;;
  *) echo "usage: $0 <up|down|logs>" >&2; exit 2 ;;
esac

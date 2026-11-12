#!/usr/bin/env bash
# Levanta los 3 nodos y el frontend sin Docker (Linux/macOS/Git Bash).
# Nodos: http://localhost:8001..8003  |  Frontend: http://localhost:8080
# Ctrl+C detiene todo.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
trap 'kill 0' EXIT

start_node() {  # id puerto peers
  (cd "$ROOT/backend" && NODE_ID=$1 PORT=$2 PEERS=$3 PUBLIC_WS_URL="ws://localhost:$2/ws" "$PY" main.py) &
}

start_node 1 8001 "2=http://localhost:8002,3=http://localhost:8003"
start_node 2 8002 "1=http://localhost:8001,3=http://localhost:8003"
start_node 3 8003 "1=http://localhost:8001,2=http://localhost:8002"
(cd "$ROOT/frontend" && "$PY" -m http.server 8080 >/dev/null 2>&1) &

echo "Frontend en http://localhost:8080"
wait

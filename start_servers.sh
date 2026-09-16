#!/usr/bin/env bash
# Starts all servers in the background, prefixing their output with the server id.
# Logs are also written to logs/server-<id>.log. Ctrl+C stops every server.
#
# Usage: ./start_servers.sh [num_servers=5] [base_port=8080] [scheduler=A]
# Build the project first with: mvn clean install
set -euo pipefail

NUM_SERVERS="${1:-5}"
BASE_PORT="${2:-8080}"
SCHEDULER="${3:-A}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$ROOT/logs"

if ! command -v mvn >/dev/null 2>&1 && [ -f "$ROOT/dev_env/env.sh" ]; then
	source "$ROOT/dev_env/env.sh"
fi

mkdir -p "$LOG_DIR"
trap 'trap - INT TERM EXIT; echo; echo "Stopping servers..."; kill 0' INT TERM EXIT

cd "$ROOT/server"
for ((id = 0; id < NUM_SERVERS; id++)); do
	echo "Starting server $id on port $((BASE_PORT + id))"
	mvn -q exec:java -Dexec.args="$BASE_PORT $id $SCHEDULER" 2>&1 |
		sed -u "s/^/[server $id] /" |
		tee "$LOG_DIR/server-$id.log" &
done

echo "All $NUM_SERVERS servers launched (scheduler $SCHEDULER). Press Ctrl+C to stop."
wait

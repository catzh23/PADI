#!/usr/bin/env bash
# Helpers for the baseline demo. Own only the processes started by this script.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-9200}"
SCHED="${SCHED:-A}"
declare -A PIDS INPUTS
STAGE=""

lab_init() {
  mkdir -p "$ROOT/demo/run"
  RUN=$(mktemp -d "$ROOT/demo/run/${MODE}-XXXXXX")
  printf 'Logs: %s\n' "$RUN"
  python3 - "$PORT" <<'PY'
import socket, sys
sockets = []
try:
    for port in range(int(sys.argv[1]), int(sys.argv[1]) + 3):
        sock = socket.socket()
        sockets.append(sock)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', port))
        sock.listen(1)
except OSError as exc:
    sys.exit('Demo ports unavailable; choose another PORT: ' + str(exc))
finally:
    for sock in sockets: sock.close()
PY
  mvn -q -f "$ROOT/server/pom.xml" dependency:build-classpath \
    -Dmdep.outputFile="$RUN/dependencies.txt" >"$RUN/classpath.log" 2>&1
  STAGE=$(mktemp -d "${TMPDIR:-/tmp}/didatrade-demo.XXXXXX")
  CP=""
  local module
  for module in server app console core configs util contract; do
    if [ ! -d "$ROOT/$module/target/classes" ]; then
      echo "Build first: mvn clean install" >&2
      return 1
    fi
    cp -R "$ROOT/$module/target/classes" "$STAGE/$module"
    CP="$CP$STAGE/$module:"
  done
  CP="$CP$(cat "$RUN/dependencies.txt")"
}

wait_log() {
  local name="$1" pattern="$2" limit="${3:-45}" end=$((SECONDS + ${3:-45}))
  while [[ ! -f "$RUN/$name.log" ]] || ! grep -Eq "$pattern" "$RUN/$name.log"; do
    if ! kill -0 "${PIDS[$name]}" 2>/dev/null; then
      echo "$name exited; inspect $RUN/$name.log" >&2
      return 1
    fi
    if (( SECONDS >= end )); then
      echo "Timeout waiting for $name: $pattern; inspect $RUN/$name.log" >&2
      return 1
    fi
    sleep 0.1
  done
}

stop_process() {
  local name="$1" fd
  if [[ -v PIDS[$name] ]]; then
    kill "${PIDS[$name]}" 2>/dev/null || true
    wait "${PIDS[$name]}" 2>/dev/null || true
    unset 'PIDS[$name]'
  fi
  if [[ -v INPUTS[$name] ]]; then
    fd=${INPUTS[$name]}
    exec {fd}>&-
    unset 'INPUTS[$name]'
  fi
}

start_server() {
  local id="$1"
  java ${JAVA_FLAGS:-} -cp "$CP" didatrade.server.DidaTradeServer "$PORT" "$id" "$SCHED" \
    >"$RUN/s$id.log" 2>&1 &
  PIDS[s$id]=$!
  wait_log "s$id" 'Server started'
}

start_terminal() {
  local name="$1" main="$2" fd
  shift 2
  # A new FIFO for every launch also permits restarting the console after a crash.
  # FIFOs must live on Linux; /mnt/c does not reliably support them.
  local fifo="$STAGE/$name-$RANDOM.in"
  mkfifo "$fifo"
  exec {fd}<>"$fifo"
  INPUTS[$name]=$fd
  java -cp "$CP" "$main" "$@" <"$fifo" >"$RUN/$name.log" 2>&1 &
  PIDS[$name]=$!
}

start_app() {
  start_terminal "app$1" didatrade.app.DidaTradeApp "$1" localhost "$PORT" "$SCHED"
  wait_log "app$1" 'app>'
}

start_console() {
  start_terminal console didatrade.console.Console localhost "$PORT" "$SCHED"
  wait_log console 'console>'
}

app() { printf '%s\n' "$2" >&"${INPUTS[app$1]}"; }
console() { printf '%s\n' "$1" >&"${INPUTS[console]}"; }

lab_stop() {
  local name
  for name in "${!PIDS[@]}"; do stop_process "$name"; done
  if [[ "$STAGE" == "${TMPDIR:-/tmp}/didatrade-demo."* ]]; then
    rm -rf -- "$STAGE"
  fi
}

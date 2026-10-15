#!/usr/bin/env bash
# Fast Paxos scenarios. Each one starts fresh servers, drives clients and the console,
# then checks the logs. Run from the repository root after `mvn clean install`:
#
#   bash validation/step3/fast_paxos.sh <scenario>     one scenario
#   bash validation/step3/fast_paxos.sh all            every scenario, with a summary
#
# Schedule B: 5 acceptors, classic quorum 3, super-majority (fast quorum) 4, recovery quorum 2.
# Scenarios (Schedule B unless noted):
#   fast       Normal run: requests decided via 'any' + fast accepts, no leader phase 2,
#              no collisions, same execution order everywhere.
#   quorum     Fast votes need a super-majority (4 of 5): with S4 frozen, 4 votes still
#              decide; with S3 and S4 frozen, 3 matching votes must NOT decide and are not
#              a collision. Un-freeze -> decided everywhere.
#   collision  Slow mode on S1 and S2 + two concurrent clients reorder requests. Expect a
#              collision, a ballot change, recovery, and identical execution everywhere.
#              Timing-dependent: if no collision happens the result is INCONCLUSIVE.
#   mixed      Schedule A (3 servers): fast ballot 0 -> classic ballot 1 -> fast ballot 3.
#   startup    S0 starts first, so acceptors may miss 'any'. Checks the 'any' retry.
#   crash      S4 crashes: the remaining 4 still form a super-majority, so requests are
#              still decided on the fast path.
#   rejoin     A non-leader is frozen while requests are decided, then un-frozen: it must
#              catch up and end in the same state.
#   leaderdown The leader is frozen after sending 'any': the fast path keeps deciding without it.
#   reballot   Manual ballot changes (console) while two clients are sending: in-flight fast
#              slots are recovered by each new leader.
#   deadleader S1 is crashed, then a collision moves everyone to ballot 1 (led by S1): the
#              system waits until the console starts ballot 2 (manual reconfiguration).
#   restart    S4 crashes and restarts with an empty state: the others keep going and S4
#              never executes anything inconsistent (it cannot catch up without state transfer).
#   stress     Three clients, slow mode, a freeze/un-freeze and a manual ballot change mid-run.
#
# Every scenario ends by checking the replicas executed the same requests in the same order,
# no request twice, and (where all replicas can execute) that their final states are identical.
#
# Exit codes: 0 pass, 1 fail, 3 inconclusive. Logs: validation/step3/runs/<scenario>-XXXXXX/.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCENARIOS=(fast quorum collision mixed startup crash rejoin leaderdown reballot deadleader restart stress)
SCENARIO="${1:-}"

if [[ "$SCENARIO" == all ]]; then
  declare -A RESULT
  for s in "${SCENARIOS[@]}"; do
    printf '\n========== %s ==========\n' "$s"
    status=0
    bash "$0" "$s" || status=$?
    case $status in 0) RESULT[$s]=PASS ;; 3) RESULT[$s]=INCONCLUSIVE ;; *) RESULT[$s]=FAIL ;; esac
  done
  printf '\n========== summary ==========\n'
  for s in "${SCENARIOS[@]}"; do printf '  %-11s %s\n' "$s" "${RESULT[$s]}"; done
  exit 0
fi

case "$SCENARIO" in
  fast | quorum | collision | startup | crash | rejoin | leaderdown | reballot | deadleader | restart | stress)
    SCHED=B; ALL=(0 1 2 3 4) ;;
  mixed) SCHED=A; ALL=(0 1 2) ;;
  *) echo "usage: $0 <${SCENARIOS[*]// /|}|all>" >&2; exit 2 ;;
esac
MODE="$SCENARIO"
PORT="${PORT:-9300}"
JAVA_FLAGS=""
if ! command -v mvn >/dev/null 2>&1 && [ -f "$ROOT/dev_env/env.sh" ]; then
  source "$ROOT/dev_env/env.sh"
fi
# ---------- process harness (inlined; previously a separate lab.sh) ----------
# Owns only the processes started by this script.
PORT="${PORT:-9200}"
SCHED="${SCHED:-A}"
declare -A PIDS INPUTS
STAGE=""

lab_init() {
  mkdir -p "$ROOT/validation/step3/runs"
  RUN=$(mktemp -d "$ROOT/validation/step3/runs/${MODE}-XXXXXX")
  printf 'Logs: %s\n' "$RUN"
  python3 - "$PORT" <<'PY'
import socket, sys
sockets = []
try:
    for port in range(int(sys.argv[1]), int(sys.argv[1]) + 7):
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
trap lab_stop EXIT
lab_init

# ---------- helpers ----------
fail() { printf '\nFAIL [%s]: %s\nLogs: %s\n' "$SCENARIO" "$*" "$RUN" >&2; exit 1; }
inconclusive() { printf '\nINCONCLUSIVE [%s]: %s\nLogs: %s\n' "$SCENARIO" "$*" "$RUN"; exit 3; }
pass() { printf '\nPASS [%s]: %s\nLogs: %s\n' "$SCENARIO" "$*" "$RUN"; exit 0; }
step() { printf '\n[%s] %s\n' "$1" "$2"; }
count() { grep -cE "$2" "$RUN/$1.log" 2>/dev/null || true; }

# Start servers in the given order (leader S0 last avoids the startup race), then the console.
start_cluster() {
  local id
  for id in "$@"; do start_server "$id"; done
  start_console
}

# All servers, highest id first, so S0 (leader of ballot 0) boots last.
start_all() {
  local ids=() i
  for ((i = ${#ALL[@]} - 1; i >= 0; i--)); do ids+=("${ALL[i]}"); done
  start_cluster "${ids[@]}"
}

# Wait until the leader has sent 'any' for a ballot, and give acceptors a moment to record it.
wait_any() {
  wait_log "s$1" "Sending 'any' (for|from) slot [0-9]+ in ballot $2" "${3:-30}"
  sleep 1
}

# Wait until server $1 has executed request $2.
wait_exec() { wait_log "s$1" "Setting response for command with id = $2 with" "${3:-45}"; }

# Wait until every listed server has executed request $1.
wait_exec_on() {
  local req="$1" limit="$2" id
  shift 2
  for id in "$@"; do wait_exec "$id" "$req" "$limit" || return 1; done
}

# Slot:command sequence executed by one server, e.g. "0:101".
executed_seq() {
  { grep -oE 'Log entry with number [0-9]+ has been decided with command id = -?[0-9]+' \
    "$RUN/$1.log" || true; } | sed -E 's/.*number ([0-9]+) .*= (-?[0-9]+)/\1:\2/'
}

# Servers must agree on every slot they have both executed (a crashed one may be shorter).
check_same_order() {
  local ref="" refname="" name seq n1 n2 n
  for name in "$@"; do
    seq=$(executed_seq "$name")
    printf '  %s executed: %s\n' "$name" "$(echo $seq)"
    if [[ -z "$refname" ]]; then ref=$seq; refname=$name; continue; fi
    n1=$(printf '%s\n' "$ref" | grep -c . || true)
    n2=$(printf '%s\n' "$seq" | grep -c . || true)
    n=$((n1 < n2 ? n1 : n2))
    if [[ "$(printf '%s\n' "$ref" | head -n "$n")" != "$(printf '%s\n' "$seq" | head -n "$n")" ]]; then
      fail "execution order differs between $refname and $name"
    fi
  done
}

# No request may be applied twice on the same server.
check_once() {
  local name dup
  for name in "$@"; do
    dup=$({ grep -oE 'Setting response for command with id = [0-9]+' "$RUN/$name.log" || true; } \
      | sort | uniq -d)
    [[ -z "$dup" ]] || fail "$name executed a request twice: $dup"
  done
}

servers() { local id; for id in "$@"; do printf 's%s ' "$id"; done; }

# Users and orders from the last `show` (dump) a server executed.
final_state() {
  awk '/----------- Users/ { buf = "" }
       /user [0-9]+ balance|order from user/ { sub(/^[ \t]+/, ""); buf = buf $0 "\n" }
       END { printf "%s", buf }' "$RUN/$1.log" | sort
}

# Client 1 asks for a dump; every listed server must print the same state.
check_same_state() {
  local before id ref="" refname="" state
  app 1 'show'
  for id in "$@"; do
    wait_log "s$id" ' Buy Orders -' 60 || fail "S$id did not execute the final dump"
  done
  sleep 1
  for id in "$@"; do
    state=$(final_state "s$id")
    [[ -n "$state" ]] || fail "S$id printed an empty state"
    if [[ -z "$refname" ]]; then ref=$state; refname=s$id; continue; fi
    [[ "$state" == "$ref" ]] || fail "final state of S$id differs from $refname"
  done
  echo "  final state identical on: $(servers "$@")($(printf '%s\n' "$ref" | grep -c 'balance') users)"
}

# Highest ballot any server has reached, from the logs.
highest_ballot() {
  { grep -hoE '(in ballot|for ballot|new ballot =) [0-9]+' "$RUN"/s?.log || echo 'ballot 0'; } \
    | grep -oE '[0-9]+$' | sort -n | tail -1
}

# Number of client requests a server has executed.
executed_count() { count "$1" 'Setting response for command with id = [0-9]+ with'; }

crash_server() {
  local id="$1" end
  console "debug crash $id"
  wait_log "s$id" 'Setting debug mode to = crash'
  end=$((SECONDS + 15))
  while kill -0 "${PIDS[s$id]}" 2>/dev/null; do
    ((SECONDS < end)) || fail "S$id did not crash"
    sleep 0.1
  done
  wait "${PIDS[s$id]}" 2>/dev/null || true
  unset "PIDS[s$id]"
}

freeze() { console "debug freeze $1"; wait_log "s$1" 'Setting debug mode to = freeze'; }
unfreeze() { console "debug un-freeze $1"; }

# ---------- scenarios ----------
scenario_fast() {
  step 1 'Start S4..S0 (leader last) and wait for any(ballot 0).'
  start_all
  start_app 1
  wait_any 0 0

  step 2 'One client sends three requests (101, 201, 301), one after another.'
  app 1 'sell 0 10'
  app 1 'buy 1 10'
  app 1 'sell 2 10'
  local id req
  for req in 101 201 301; do wait_exec_on "$req" 45 "${ALL[@]}"; done

  step 3 'Check the fast path was used.'
  for id in "${ALL[@]}"; do
    for req in 101 201 301; do
      (($(count "s$id" "Fast accepted request $req in slot [0-9]+ in ballot 0") == 1)) \
        || fail "S$id did not fast-accept $req exactly once in ballot 0"
    done
    (($(count "s$id" 'Sending phase 2') == 0)) || fail "S$id sent a classic phase 2"
    (($(count "s$id" 'Collision detected') == 0)) || fail "S$id saw a collision in a sequential run"
  done
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  check_same_state "${ALL[@]}"
  pass 'requests decided by fast accepts only, same order on all replicas.'
}

scenario_quorum() {
  local id
  step 1 'Start the cluster and wait for any(ballot 0).'
  start_all
  start_app 1
  wait_any 0 0

  step 2 'Freeze S4. The other 4 acceptors are exactly a super-majority: 101 must be decided.'
  freeze 4
  app 1 'sell 0 10'
  wait_exec_on 101 45 0 1 2 3 || fail '101 not decided with 4 of 5 fast votes'
  echo '  OK: decided with 4 of 5 votes.'

  step 3 'Also freeze S3. Only S0, S1, S2 can vote 201: 3 < 4, so it must NOT be decided.'
  freeze 3
  app 1 'sell 1 10'
  for id in 0 1 2; do wait_log "s$id" 'Fast accepted request 201 in slot [0-9]+ in ballot 0'; done
  sleep 3
  for id in 0 1 2; do
    (($(count "s$id" 'Setting response for command with id = 201 with') == 0)) \
      || fail "S$id executed 201 with only 3 fast votes"
    (($(count "s$id" 'Collision detected') == 0)) \
      || fail "S$id reported a collision although S3/S4 could still vote 201"
  done
  echo '  OK: 201 undecided with 3/5 votes, no collision.'

  step 4 'Un-freeze S3 and S4: their votes complete the super-majority.'
  unfreeze 3
  unfreeze 4
  wait_exec_on 201 45 "${ALL[@]}" || fail '201 not executed everywhere after un-freeze'
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  pass 'fast decisions need exactly a super-majority (4 of 5).'
}

scenario_collision() {
  local rounds=6 id i
  step 1 'Start the cluster, two clients, and wait for any(ballot 0).'
  start_all
  start_app 1
  start_app 2
  wait_any 0 0

  step 2 'Slow mode on S1 and S2 (random 0.5-4 s delay per incoming request).'
  # With a super-majority of 4, at least one slow acceptor must vote on every slot.
  for id in 1 2; do
    console "debug slow-mode-on $id"
    wait_log "s$id" 'Setting debug mode to = slow-mode-on'
  done

  step 3 "Both clients send $rounds requests concurrently."
  for ((i = 0; i < rounds; i++)); do
    app 1 'sell 0 5'
    app 2 'buy 1 5'
  done
  wait_exec_on $((rounds * 100 + 1)) 300 "${ALL[@]}" || fail 'client 1 requests not all executed'
  wait_exec_on $((rounds * 100 + 2)) 300 "${ALL[@]}" || fail 'client 2 requests not all executed'

  step 4 'Check collisions were recovered and replicas agree.'
  local collisions=0
  for id in "${ALL[@]}"; do collisions=$((collisions + $(count "s$id" 'Collision detected'))); done
  echo "  collision log lines: $collisions"
  for ((i = 1; i <= rounds; i++)); do
    for id in "${ALL[@]}"; do
      (($(count "s$id" "Setting response for command with id = $((i * 100 + 1)) with") == 1)) \
        || fail "S$id did not execute $((i * 100 + 1))"
      (($(count "s$id" "Setting response for command with id = $((i * 100 + 2)) with") == 1)) \
        || fail "S$id did not execute $((i * 100 + 2))"
    done
  done
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  check_same_state "${ALL[@]}"
  if ((collisions == 0)); then
    inconclusive 'all requests executed consistently, but no collision happened this run; rerun.'
  fi
  grep -qE 'Going to run paxos phase 1 for ballot [1-9]' "$RUN"/s?.log \
    || fail 'collision detected but no server started phase 1 for a higher ballot'
  grep -hoE 'Multi-prepare completed for ballot [0-9]+; recovered slots = .*' "$RUN"/s?.log \
    | sort -u | sed 's/^/  /'
  pass 'collisions detected, ballot changed, all requests executed once in the same order.'
}

scenario_mixed() {
  local id
  step 1 'Schedule A. Ballot 0 is fast (S0): request 101 must be fast-accepted.'
  start_all
  start_app 1
  wait_any 0 0
  app 1 'sell 0 10'
  wait_exec_on 101 45 "${ALL[@]}"
  wait_log s0 'Fast accepted request 101 in slot 0 in ballot 0'

  step 2 'Ballot 1 is classic (S1): request 201 must go through phase 2, not fast accepts.'
  console 'ballot 1 1'
  wait_log s1 'Multi-prepare completed for ballot 1'
  app 1 'sell 1 10'
  wait_log s1 'Sending phase 2 for slot 1 in ballot 1, value 201'
  wait_exec_on 201 45 "${ALL[@]}"
  for id in "${ALL[@]}"; do
    (($(count "s$id" 'Fast accepted request 201') == 0)) || fail "S$id fast-accepted in classic ballot 1"
  done

  step 3 'Ballot 3 is fast again (S0): any(3, slot 2), then request 301 fast-accepted.'
  console 'ballot 3 0'
  wait_log s0 'Multi-prepare completed for ballot 3'
  wait_any 0 3
  app 1 'sell 2 10'
  wait_exec_on 301 45 "${ALL[@]}"
  for id in "${ALL[@]}"; do
    (($(count "s$id" 'Fast accepted request 301 in slot 2 in ballot 3') == 1)) \
      || fail "S$id did not fast-accept 301 in slot 2, ballot 3"
  done
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  check_same_state "${ALL[@]}"
  pass 'fast -> classic -> fast transitions keep one consistent log.'
}

scenario_startup() {
  local id
  step 1 'Start S0 FIRST: its eager phase 1 can succeed before the others are up.'
  start_cluster "${ALL[@]}"
  start_app 1
  sleep 2

  step 2 "Send request 101: it needs fast votes from a super-majority."
  app 1 'sell 0 10'
  wait_exec_on 101 20 "${ALL[@]}" \
    || fail "101 not executed everywhere within 20 s. Likely acceptors missed 'any' and it was never resent."
  for id in "${ALL[@]}"; do
    (($(count "s$id" 'Fast accepted request 101') == 1)) || fail "S$id never fast-accepted 101 (missed 'any')"
  done
  echo "  'any' sends by S0: $(count s0 "Sending 'any'")"
  check_same_order $(servers "${ALL[@]}")
  pass "every acceptor got 'any' despite the startup order."
}

scenario_crash() {
  step 1 'Start the cluster and decide request 101 with all 5 servers.'
  start_all
  start_app 1
  wait_any 0 0
  app 1 'sell 0 10'
  wait_exec_on 101 45 "${ALL[@]}"

  step 2 'Crash S4. S0..S3 are still a super-majority (4).'
  crash_server 4

  step 3 'Send request 201: it must still be decided on the fast path.'
  app 1 'sell 1 10'
  wait_exec_on 201 30 0 1 2 3 || fail '201 not executed on S0..S3 after one crash'
  (($(count s0 'Sending phase 2') == 0)) || fail 'S0 fell back to classic phase 2'
  check_same_order s0 s1 s2 s3 s4
  check_once s0 s1 s2 s3
  check_same_state 0 1 2 3
  pass 'one crash tolerated on the fast path.'
}

scenario_rejoin() {
  local req
  step 1 'Start the cluster and wait for any(ballot 0).'
  start_all
  start_app 1
  wait_any 0 0

  step 2 'Freeze S3, then decide 101, 201, 301 with the other 4 acceptors.'
  freeze 3
  app 1 'sell 0 10'
  app 1 'buy 1 20'
  app 1 'sell 2 30'
  for req in 101 201 301; do wait_exec_on "$req" 45 0 1 2 4 || fail "$req not decided without S3"; done
  (($(executed_count s3) == 0)) || fail 'frozen S3 executed something'

  step 3 'Un-freeze S3: it must catch up on all three.'
  unfreeze 3
  for req in 101 201 301; do wait_exec_on "$req" 45 3 || fail "S3 did not catch up on $req"; done

  step 4 'Same with S4: freeze, decide 401, un-freeze, catch up.'
  freeze 4
  app 1 'buy 3 40'
  wait_exec_on 401 45 0 1 2 3 || fail '401 not decided without S4'
  unfreeze 4
  wait_exec_on 401 45 4 || fail 'S4 did not catch up on 401'

  step 5 'Compare logs and final states.'
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  check_same_state "${ALL[@]}"
  pass 'frozen replicas catch up and converge to the same state.'
}

scenario_leaderdown() {
  step 1 'Start the cluster and wait for any(ballot 0) from S0.'
  start_all
  start_app 1
  wait_any 0 0

  step 2 'Freeze the leader S0. Acceptors S1..S4 already have any: the fast path needs no leader.'
  freeze 0
  app 1 'sell 0 10'
  app 1 'buy 1 10'
  wait_exec_on 201 45 1 2 3 4 || fail 'requests not decided while the leader was frozen'
  (($(executed_count s0) == 0)) || fail 'frozen S0 executed something'

  step 3 'Un-freeze S0: it catches up; no ballot change was needed.'
  unfreeze 0
  wait_exec_on 201 45 0 || fail 'S0 did not catch up'
  ! grep -qE 'Going to run paxos phase 1 for ballot [1-9]' "$RUN"/s?.log \
    || fail 'an unexpected ballot change happened'
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  check_same_state "${ALL[@]}"
  pass 'with the leader frozen, fast decisions continued (2 message delays, no leader).'
}

scenario_reballot() {
  local rounds=8 i
  step 1 'Start the cluster, two clients, and wait for any(ballot 0).'
  start_all
  start_app 1
  start_app 2
  wait_any 0 0

  step 2 "Both clients send $rounds requests; meanwhile the console starts ballots 1, 2 and 3."
  for ((i = 0; i < rounds; i++)); do
    app 1 "sell $((i % 10)) 10"
    app 2 "buy $(((i + 5) % 10)) 10"
  done
  sleep 1; console 'ballot 1 1'
  sleep 2; console 'ballot 2 2'
  sleep 2; console 'ballot 3 3'
  wait_exec_on $((rounds * 100 + 1)) 120 "${ALL[@]}" || fail 'client 1 requests not all executed'
  wait_exec_on $((rounds * 100 + 2)) 120 "${ALL[@]}" || fail 'client 2 requests not all executed'

  step 3 'Check every request executed once, same order, same state.'
  grep -hoE 'Multi-prepare completed for ballot [0-9]+; recovered slots = .*' "$RUN"/s?.log \
    | sort -u | sed 's/^/  /'
  (($(executed_count s0) == 2 * rounds)) || fail "S0 executed $(executed_count s0), expected $((2 * rounds))"
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  check_same_state "${ALL[@]}"
  pass 'leader changes during traffic preserved every decision.'
}

scenario_deadleader() {
  local rounds=5 i id
  step 1 'Start the cluster and wait for any(ballot 0). Crash S1, the leader of ballot 1.'
  start_all
  start_app 1
  start_app 2
  wait_any 0 0
  crash_server 1

  step 2 'Slow mode on S2 and S3, two concurrent clients: provoke a collision.'
  for id in 2 3; do
    console "debug slow-mode-on $id"
    wait_log "s$id" 'Setting debug mode to = slow-mode-on'
  done
  for ((i = 0; i < rounds; i++)); do
    app 1 'sell 0 5'
    app 2 'buy 1 5'
  done
  if ! wait_log s0 'Collision detected' 120; then
    inconclusive 'no collision happened this run; rerun.'
  fi

  step 3 'Everyone moved to ballot 1, whose leader S1 is dead: no progress is expected.'
  sleep 6
  local stuck=$(executed_count s0)
  sleep 4
  (($(executed_count s0) == stuck)) || echo '  (note: progress continued; the collision may have been in the last slots)'
  ! grep -q 'Going to run paxos phase 1 for ballot 1' "$RUN"/s?.log || fail 'someone ran ballot 1'

  step 4 'Manual reconfiguration: whenever the live servers stall, the console starts the next'
  echo '         ballot whose leader is alive (a 3-1 split with S1 dead is not detectable).'
  local target1=$((rounds * 100 + 1)) target2=$((rounds * 100 + 2)) manual=0 next last progress
  last=-1; progress=$SECONDS
  while ! { grep -q "Setting response for command with id = $target1 with" "$RUN/s0.log" \
            && grep -q "Setting response for command with id = $target2 with" "$RUN/s0.log"; }; do
    ((manual <= 15)) || fail 'still stalled after 15 manual ballots'
    if (($(executed_count s0) != last)); then last=$(executed_count s0); progress=$SECONDS; fi
    if ((SECONDS - progress >= 8)) || ((manual == 0)); then
      next=$(($(highest_ballot) + 1))
      while ((next % 5 == 1)); do next=$((next + 1)); done   # skip ballots led by the dead S1
      console "ballot $next $((next % 5))"
      echo "  stalled: console starts ballot $next at S$((next % 5))"
      manual=$((manual + 1)); progress=$SECONDS
    fi
    sleep 1
  done
  wait_exec_on "$target1" 60 0 2 3 4 || fail 'client 1 requests not all executed after recovery'
  wait_exec_on "$target2" 60 0 2 3 4 || fail 'client 2 requests not all executed after recovery'
  echo "  manual ballots needed: $manual"
  grep -hoE 'Multi-prepare completed for ballot [0-9]+; recovered slots = .*' "$RUN"/s?.log | sort -u | sed 's/^/  /'
  check_same_order s0 s2 s3 s4
  check_once s0 s2 s3 s4
  check_same_state 0 2 3 4
  pass 'with S1 dead, collisions stall until the console starts a new ballot; then all requests execute consistently.'
}

scenario_restart() {
  step 1 'Start the cluster and decide 101 everywhere.'
  start_all
  start_app 1
  wait_any 0 0
  app 1 'sell 0 10'
  wait_exec_on 101 45 "${ALL[@]}"

  step 2 'Crash S4, decide 201 without it, then restart S4 with an empty state.'
  crash_server 4
  app 1 'buy 1 10'
  wait_exec_on 201 45 0 1 2 3 || fail '201 not decided after S4 crashed'
  mv "$RUN/s4.log" "$RUN/s4-before-restart.log"
  start_server 4

  step 3 'Decide 301; then a manual ballot change gives the new S4 an any, and decide 401.'
  app 1 'sell 2 10'
  wait_exec_on 301 45 0 1 2 3 || fail '301 not decided after S4 restarted'
  console 'ballot 1 1'
  wait_log s1 'Multi-prepare completed for ballot 1' 60 || fail 'ballot 1 did not start'
  app 1 'buy 3 10'
  wait_exec_on 401 45 0 1 2 3 || fail '401 not decided after the ballot change'

  step 4 'The others agree; the restarted S4 must not have executed anything out of order.'
  echo "  restarted S4 executed $(executed_count s4) request(s) (it lacks the history: no state transfer)"
  check_same_order s0 s1 s2 s3 s4
  check_once s0 s1 s2 s3 s4
  check_same_state 0 1 2 3
  pass 'a restarted empty replica did not disturb the others or diverge.'
}

scenario_stress() {
  local rounds=10 i c
  step 1 'Start the cluster, three clients, and wait for any(ballot 0).'
  start_all
  for c in 1 2 3; do start_app "$c"; done
  wait_any 0 0
  console 'debug slow-mode-on 2'
  wait_log s2 'Setting debug mode to = slow-mode-on'

  step 2 "Three clients send $rounds requests each; S3 is frozen and un-frozen, S1 takes over."
  for ((i = 0; i < rounds; i++)); do
    app 1 "sell $((i % 10)) 15"
    app 2 "buy $(((i + 3) % 10)) 25"
    app 3 "$([[ $((i % 2)) == 0 ]] && echo sell || echo buy) $(((i + 7) % 10)) 35"
  done
  sleep 2; freeze 3
  sleep 5; console 'ballot 1 1'
  sleep 3; unfreeze 3
  for c in 1 2 3; do
    wait_exec_on $((rounds * 100 + c)) 300 "${ALL[@]}" || fail "client $c requests not all executed"
  done

  step 3 'Check every request executed once, same order, same state.'
  local collisions=0 id
  for id in "${ALL[@]}"; do collisions=$((collisions + $(count "s$id" 'Collision detected'))); done
  echo "  collision log lines: $collisions"
  grep -hoE 'Multi-prepare completed for ballot [0-9]+' "$RUN"/s?.log | sort -u | sed 's/^/  /'
  (($(executed_count s0) == 3 * rounds)) || fail "S0 executed $(executed_count s0), expected $((3 * rounds))"
  check_same_order $(servers "${ALL[@]}")
  check_once $(servers "${ALL[@]}")
  check_same_state "${ALL[@]}"
  pass 'concurrency, slow mode, a freeze and a leader change: one consistent replicated state.'
}

"scenario_$SCENARIO"

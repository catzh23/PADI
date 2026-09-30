#!/usr/bin/env bash
set -euo pipefail
MODE="${1:-bogus}"
case "$MODE" in
  bogus) JAVA_FLAGS="-Ddidatrade.phase1=bogus" ;;
  fixed) JAVA_FLAGS="" ;;
  *) echo "usage: $0 [bogus|fixed]" >&2; exit 2 ;;
esac
source "$(dirname "$0")/lab.sh"
trap lab_stop EXIT
lab_init

printf '\n[1] Start three replicas and decide request 101 at slot 0.\n'
for id in 0 1 2; do start_server "$id"; done
start_app 1
start_app 2
start_console
app 1 'sell 0 50'
for id in 0 1 2; do
  wait_log "s$id" 'Log entry with number 0 has been decided with command id = 101'
done

printf '\n[2] Crash and restart S1 with an empty log.\n'
console 'debug crash 1'
wait_log s1 'Setting debug mode to = crash'
end=$((SECONDS + 15))
while kill -0 "${PIDS[s1]}" 2>/dev/null; do
  if (( SECONDS >= end )); then echo 'S1 did not crash' >&2; exit 1; fi
  sleep 0.1
done
wait "${PIDS[s1]}" 2>/dev/null || true
unset 'PIDS[s1]'
mv "$RUN/s1.log" "$RUN/s1-before-restart.log"
start_server 1
# New connections avoid racing the old channel's reconnect backoff after the crash.
stop_process console
mv "$RUN/console.log" "$RUN/console-before-restart.log"
start_console

printf '\n[3] Freeze S0/S2: only the empty S1 can answer prepare immediately.\n'
console 'debug freeze 0'
wait_log s0 'Setting debug mode to = freeze'
console 'debug freeze 2'
wait_log s2 'Setting debug mode to = freeze'
console 'ballot 1 1'
wait_log s1 'new ballot = 1'
wait_log s1 'Going to run paxos phase 1 for ballot 1'

if [[ "$MODE" == bogus ]]; then
  wait_log s1 'using the BOGUS processor'
  wait_log s1 'Multi-prepare completed for ballot 1; recovered slots = \[\]'
fi
printf '\n[4] Send request 102. The old decision at slot 0 must remain 101.\n'
app 2 'sell 3 50'
wait_log s1 'Adding sell request with reqid 102 to pending'
if [[ "$MODE" == bogus ]]; then
  wait_log s1 'Sending phase 2 for slot 0 in ballot 1, value 102'
else
  if grep -q 'Multi-prepare completed for ballot 1' "$RUN/s1.log"; then
    echo 'FAIL: fixed prepare completed while only one acceptor could respond' >&2
    exit 1
  fi
  echo 'PASS: fixed prepare is still waiting for a majority.'
fi

printf '\n[5] Unfreeze the surviving acceptors and inspect the decision.\n'
console 'debug un-freeze 0'
wait_log s0 'Setting debug mode to = un-freeze'
console 'debug un-freeze 2'
wait_log s2 'Setting debug mode to = un-freeze'
if [[ "$MODE" == bogus ]]; then
  wait_log s1 'Log entry with number 0 has been decided with command id = 102'
  printf '\nPASS: baseline defect reproduced. Slot 0 executed as 101 on S0/S2 and 102 on restarted S1.\n'
else
  wait_log s1 'Multi-prepare completed for ballot 1'
  wait_log s1 'Sending phase 2 for slot 0 in ballot 1, value 101'
  wait_log s1 'Phase 2 chose slot 0 in ballot 1|Paxos learner decided slot 0 in ballot 1, value 101'
  if grep -Eq 'Sending phase 2 for slot 0 in ballot 1, value 102|Log entry with number 0 has been decided with command id = 102' "$RUN/s1.log"; then
    echo 'FAIL: fixed mode replaced the old value' >&2
    exit 1
  fi
  printf '\nPASS: fixed phase 1 recovered 101 and slot 0 was decided with that value again.\n'
  printf 'This checks consensus, not execution after restart: S1 still lacks the body of request 101.\n'
fi
printf 'Logs: %s\n' "$RUN"

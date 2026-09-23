#!/usr/bin/env bash
set -uo pipefail

MODE="${1:-bogus}"
case "$MODE" in
bogus) export JAVA_FLAGS="-Ddidatrade.phase1=bogus" ;;
fixed) export JAVA_FLAGS="" ;;
*)
	echo "usage: $0 [bogus|fixed]" >&2
	exit 2
	;;
esac

source "$(dirname "$0")/lab.sh"

step() { printf '\n\033[1m>>> %s\033[0m\n' "$*"; }

trap 'lab_stop' EXIT
lab_stop
sleep 1
lab_init

step "mode: $MODE (JAVA_FLAGS='${JAVA_FLAGS}')"
step "starting 3 servers (Schedule A, ballot 0 -> leader S0), 2 clients, console"
for i in 0 1 2; do start_server $i; done
sleep 4
start_app 1
start_app 2
start_console
sleep 4

step "[1] client 1 sells. This is log slot 0, decided by all three at ballot 0."
app 1 "sell 0 50"
sleep 4

step "[2] crash S1 and restart it. It comes back with an EMPTY Paxos log:"
step "    nothing in the baseline transfers past decisions to a restarted replica."
console "debug crash 1"
sleep 3
start_server 1
sleep 4

step "[3] slow S0 and S2 so the restarted S1 wins the phase-1 reply race."
step "    (bogus phase 1 stops at the FIRST reply, so whoever answers first decides.)"
console "debug slow-mode-on 0"
sleep 1
console "debug slow-mode-on 2"
sleep 1

step "[4] hand leadership of ballot 1 to S1."
console "ballot 1 1"
sleep 3

step "[5] client 2 sells. S1 proposes it for ITS slot 0 -- the slot already holding V1."
app 2 "sell 3 50"
sleep 12

console "debug slow-mode-off 0"
sleep 1
console "debug slow-mode-off 2"
sleep 2
app 1 "show"
sleep 5

app 1 "exit"
app 2 "exit"
console "exit"
sleep 1
lab_stop
sleep 1

# ---------------------------------------------------------------- verdict
slot0() { grep -m1 "Log entry with number 0 has been decided" "$RUN/s$1.log" | grep -o '[0-9]*$'; }
V0=$(slot0 0)
V1=$(slot0 1)
V2=$(slot0 2)

printf '\n\033[1m================ RESULT (%s) ================\033[0m\n' "$MODE"
printf 'command executed at log slot 0:\n'
printf '  S0 -> %s\n  S1 -> %s\n  S2 -> %s\n' "${V0:-none}" "${V1:-none}" "${V2:-none}"
printf '\nS1 phase 1 for slot 0:\n  '
grep -m1 "Paxos phase 1 ended" "$RUN/s1.log" | tail -1
printf '\nstate machine effects:\n'
for i in 0 1 2; do
	printf '  S%s: %s\n' "$i" "$(grep -o 'in sell for user [0-9]*' "$RUN/s$i.log" | tr '\n' ';')"
done
printf '\n'

if [ "$MODE" = "bogus" ]; then
	if [ -n "${V0:-}" ] && [ -n "${V1:-}" ] && [ "$V0" != "$V1" ]; then
		printf '\033[31mSAFETY VIOLATED (as expected on the baseline)\033[0m\n'
		printf 'S0 and S1 applied different commands at slot 0. A correct phase 1 would have read\n'
		printf 'a quorum (2 of 3), necessarily including S0 or S2, seen valballot=0 / value=%s,\n' "$V0"
		printf 'and been FORCED to re-propose it.\n'
		exit 0
	fi
	printf 'Did not reproduce this run (timing). Re-run; logs in %s\n' "$RUN"
	exit 1
else
	if [ -n "${V0:-}" ] && [ "$V0" = "${V1:-}" ] && [ "$V0" = "${V2:-}" ]; then
		printf '\033[32mSAFETY HELD\033[0m: all three replicas applied %s at slot 0.\n' "$V0"
		printf 'S1 phase 1 read a quorum, recovered the value accepted at ballot 0, and re-proposed it.\n'
		exit 0
	fi
	printf '\033[31mREGRESSION\033[0m: replicas disagree at slot 0 with the fix in. Logs in %s\n' "$RUN"
	exit 1
fi

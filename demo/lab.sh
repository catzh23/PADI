#!/usr/bin/env bash
# Lab harness: launch servers / clients / console as background processes driven by FIFOs,
# so a whole scenario can be scripted instead of typed by hand.
#
# Not used by the application itself -- this exists only to reproduce demos.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="${RUN:-$ROOT/demo/run}"
PORT="${PORT:-9200}"
SCHED="${SCHED:-A}"
JAVA_FLAGS="${JAVA_FLAGS:-}"

if ! command -v mvn >/dev/null 2>&1 && [ -f "$ROOT/dev_env/env.sh" ]; then
	source "$ROOT/dev_env/env.sh"
fi

# Classpath: module classes + every jar Maven already downloaded. Avoids needing
# maven-dependency-plugin, which is not in the offline repo.
build_classpath() {
	local cp="" m
	for m in server app console core configs util contract; do
		cp="$cp$ROOT/$m/target/classes:"
	done
	if [ ! -d "$ROOT/server/target/classes" ]; then
		echo "demo: build first -- run 'mvn clean install' in $ROOT" >&2
		exit 1
	fi
	cp="$cp$(find "${M2_REPO:-$HOME/.m2/repository}" -name '*.jar' \
		! -name '*sources*' ! -name '*javadoc*' | tr '\n' ':')"
	echo "$cp"
}
CP="$(build_classpath)"

lab_init() {
	rm -rf "$RUN"
	mkdir -p "$RUN"
}

start_server() { # start_server <id>
	java $JAVA_FLAGS -cp "$CP" didatrade.server.DidaTradeServer "$PORT" "$1" "$SCHED" \
		>"$RUN/s$1.log" 2>&1 &
}

start_app() { # start_app <id>
	mkfifo "$RUN/app$1.in"
	tail -f "$RUN/app$1.in" |
		java -cp "$CP" didatrade.app.DidaTradeApp "$1" localhost "$PORT" "$SCHED" \
			>"$RUN/app$1.log" 2>&1 &
}

start_console() {
	mkfifo "$RUN/console.in"
	tail -f "$RUN/console.in" |
		java -cp "$CP" didatrade.console.Console localhost "$PORT" "$SCHED" \
			>"$RUN/console.log" 2>&1 &
}

app() { echo "$2" >"$RUN/app$1.in"; }   # app <client-id> "<command>"
console() { echo "$1" >"$RUN/console.in"; }

lab_stop() {
	pkill -f 'didatrade.server.DidaTradeServer' 2>/dev/null
	pkill -f 'didatrade.app.DidaTradeApp' 2>/dev/null
	pkill -f 'didatrade.console.Console' 2>/dev/null
	pkill -f "tail -f $RUN" 2>/dev/null
	true
}

# Validation of Parts 1 and 2

This directory contains standalone checks for the compiled project. Despite the
historical `part1` directory name, it also covers multi-prepare and pipelining.
The tests do not change application source files.

## Requirements

Run in Ubuntu or WSL with Java 22 (including `javac`), Maven and Python 3.
No additional Python packages are required. Maven may need network access to
resolve dependencies on the first run.

## Build and run

From the repository root, compile all modules first:

```bash
mvn clean install
```

Continue only after `BUILD SUCCESS`. Run these four scenarios separately:

```bash
python3 validation/part1/run.py processors
python3 validation/part1/run.py multiprepare
python3 validation/part1/run.py pipeline
python3 validation/part1/run.py debug
```

These checks are not automatically executed by `mvn clean install`.
Rebuild after changing production Java or `.proto` files. Avoid concurrent IDE
builds or Maven builds writing to the same `target` directories during compilation
and test startup.

## What each scenario checks

| Scenario | Coverage |
|---|---|
| `processors` | Phase 1 and phase 2 quorums, distinct acceptors, rejection handling, highest accepted ballot selection, and communication failures without sufficient positive responses. |
| `multiprepare` | Recovery of multiple slots, highest accepted ballot per slot, the requested lower bound, global promises, duplicate learner notifications, no-ops, and preparation after a ballot change. |
| `pipeline` | A later slot can be decided while replies for an earlier slot are held; execution remains ordered. Also checks ballot changes with proposals in flight, late replies, retries, request assignment and the bounded proposal window. |
| `debug` | Three real servers: sell/buy replication, balances 15 and 5, freeze/un-freeze, slow-mode-on/off, and continued operation with one crashed server. |

The processor checks exercise the real processors and collectors. The
multi-prepare checks exercise the acceptor service and the real leader loop with
controlled gRPC peers. The pipeline checks use controlled peers to hold responses
and inject failures; they do not depend only on timing measurements to establish
that proposals overlap. The debug scenario starts actual server, client and
console processes.

## Expected output and logs

For the four scenarios above, each assertion should print `PASS`. A successful
scenario exits with code 0. A failed assertion or scenario error exits with code 1.
Check the exit code immediately after a command with:

```bash
echo $?
```

Every invocation creates a new directory under `validation/part1/runs/`, prints
its path, and writes a `results.json` summary plus detailed logs. For Java checks,
read `processors.log`, `multiprepare.log` or `pipeline.log` for individual
assertions and exceptions. For `debug`, inspect the per-process logs in its
`debug/` subdirectory. The runner's printed summary may omit exception details
that are retained in the log.

A failure labelled `harness/scenario completed` can indicate a build, startup,
port or timeout problem. Inspect the logs before interpreting it as a protocol
failure. Passing these checks is evidence for the scenarios exercised, not a
proof covering every failure schedule.

The runner obtains its dependency classpath through Maven and stages compiled
classes on the Linux filesystem. Temporary staged classes are cleaned up, while
logs are retained. It terminates only the subprocesses it starts.

## Ports

The `debug` scenario uses ports 19340--19342. If they are occupied, use:

```bash
python3 validation/part1/run.py debug --port 19440
```

The controlled gRPC peers in the Java scenarios use automatically assigned ports.
Run the scenarios sequentially as shown above.

## Baseline demonstration

Use the maintained demo for the bogus/fixed comparison:

```bash
bash demo/bogus_phase1.sh bogus
bash demo/bogus_phase1.sh fixed
```

See [the demo README](../../demo/README.md) for the scenario and expected results.
Both demo modes exit 0 when their expected outcome is observed: divergence for
`bogus`, and preservation of the previous decision for `fixed`.

The Python runner still contains legacy `bogus` and `fixed` diagnostic scenarios
based on the old restart demonstration. Their log assumptions and verdicts have
not been updated for the current implementation. **Do not use them as acceptance
tests, and do not use `run.py all` for the checkpoint:** `all` includes those legacy
scenarios. Use the four explicit commands above and the maintained demo instead.

## Scope and limitations

These checks do not validate Fast Paxos, dynamic membership or durable crash
recovery. A follower can still stall after losing learner notifications, and a
restarted replica can recover a decided request ID without recovering its command
body. Those are known limitations, not behaviours validated as solved here.

## Files to commit

Commit `run.py`, the three Java check files, this README and `.gitignore`.
The local `.gitignore` excludes `runs/`, `classpath.txt` and `__pycache__/`.
Do not commit generated logs, result directories, compiled classes or Maven build
output. Production source changes and the scripts under `demo/` are separate from
this validation directory and must also be present in the repository for the
corresponding commands to work.

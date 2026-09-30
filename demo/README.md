# Demos: showing the baseline is bogus

Project Step 1 asks students to *"implement debug modes required to show that the
baseline is bogus"*. This directory holds a scripted, self-checking reproduction.

Run from the repository root in Ubuntu/WSL, with Java 22, Maven and Python 3:

```bash
mvn clean install                     # build all modules
bash demo/bogus_phase1.sh bogus        # baseline phase 1 -> expect divergence
bash demo/bogus_phase1.sh fixed        # corrected phase 1 -> expect the old decision to be preserved
```

Both exit 0 when the expected outcome is observed, so the pair works as a regression
test. `bogus` selects the old processor with `-Ddidatrade.phase1=bogus`;
`PhaseOneBogusProcessor` is kept in the tree for exactly this reason.
The normal implementation uses `PhaseOneResponseProcessor`.

Per-process logs land in a separate directory for each run under `demo/run/`
(gitignored). The script prints the directory and preserves previous runs.

## What is bogus

`util/src/main/java/didatrade/util/PhaseOneBogusProcessor.java` retains the
incorrect phase 1 logic. Its relevant behaviour is illustrated below; this is an
abbreviated excerpt, omitting the multi-prepare response map:

```java
this.accepted = true;                     // set once, never reassigned
// In onNext(..., last_response):
this.maxballot = last_response.getMaxballot();
this.value     = last_response.getValue();
this.valballot = last_response.getValballot();
return true;                              // "done" after the FIRST reply
```

Three separate defects:

1. It never reads `last_response.getAccepted()`, so the processor can report
   success even when the response rejects the ballot.
2. It returns `true` on the first reply, so phase 1 never waits for a **quorum**.
3. It copies the response's accepted value and ballot without comparing accepted
   ballots across responses. In the multi-prepare interface, it also replaces
   the recovered map rather than merging the highest accepted ballot per slot.

Reading only one acceptor can miss a value that an earlier ballot already got
accepted by a quorum. The proposer may then choose a different value for the same
slot. This demo directly reproduces that failure. It does not separately exercise
an explicit rejection or comparison between two different accepted ballots.

## What the script does

Schedule A, 3 servers, ballot *b* has leader `b % 3`.

| Step | Action | Why |
|---|---|---|
| 1 | Client 1 runs `sell 0 50` | Request `101` is decided at slot 0 in ballot 0. The script waits for all three replicas to execute it. |
| 2 | `debug crash 1`, then relaunch S1 | S1 returns with an **empty** `PaxosLog`: its in-memory history was lost. |
| 3 | `debug freeze 0` and `debug freeze 2` | Only the restarted, empty S1 can answer phase 1 immediately. |
| 4 | `ballot 1 1` | S1 becomes leader of ballot 1. The script checks that the command arrived and preparation started. |
| 5 | Client 2 runs `sell 3 50` | Request `102` becomes pending. Bogus phase 1 permits S1 to propose it for slot 0, which previously held `101`. |
| 6 | `debug un-freeze 0` and `debug un-freeze 2` | The other acceptors resume processing, allowing the outstanding rounds to complete. |

The script uses the required `crash`, `freeze` and `un-freeze` debug modes.
Freezing the other acceptors makes the availability of their responses explicit,
instead of depending on random slow-mode delays. In fixed mode, the script checks
that preparation has not completed before un-freezing them.

## Result

In bogus mode, the observed execution history is:

```text
command executed at log slot 0:
  S0 -> 101
  S1 -> 102
  S2 -> 101
```

Relevant messages in the restarted S1's log are:

```text
Multi-prepare completed for ballot 1; recovered slots = []
Sending phase 2 for slot 0 in ballot 1, value 102
Log entry with number 0 has been decided with command id = 102
```

S1 consulted exactly one acceptor -- itself, freshly restarted and empty --
concluded that no value needed to be preserved for slot 0, and proposed the new
request. S0 and S2 had already applied `sell 0 50` at slot 0; S1 applies
`sell 3 50` there. This is a disagreement in the replicated execution history.

A correct phase 1 waits for a quorum. With 3 acceptors, any quorum of 2 must contain
S0 or S2. Both retain the acceptance `valballot = 0, value = 101` for slot 0,
so the leader is **forced to re-propose `101`**.

The bogus run reports:

```text
PASS: baseline defect reproduced. Slot 0 executed as 101 on S0/S2 and 102 on restarted S1.
```

Here, `PASS` means the demo reproduced the defect, not that bogus Paxos is correct.

## After the fix

`bash demo/bogus_phase1.sh fixed` runs the same scenario with the corrected processor.
While S0 and S2 are frozen, S1's own promise is insufficient to complete phase 1.
After un-freeze, S1 obtains a quorum and recovers the original value:

```text
Multi-prepare completed for ballot 1; recovered slots = [0]
Sending phase 2 for slot 0 in ballot 1, value 101
```

The script also waits for a decision notification for slot 0 in ballot 1, either
from the proposer's phase 2 collector or the learner, and checks that S1 did not
propose or execute `102` for that slot. It reports:

```text
PASS: fixed phase 1 recovered 101 and slot 0 was decided with that value again.
```

**What this then exposes.** S1 recovers which request ID was decided, but cannot
execute that request after restarting: it lost the command body, which client 1
sent before the crash. Consensus on the order is not enough when the value is
only a request ID; the replica also needs the command. The fixed run therefore
checks preservation of the decision, not successful execution on the restarted
replica.

This scenario deliberately restarts an acceptor without its in-memory history.
It is not a demonstration of durable Paxos crash recovery or state transfer.

## Reproducibility

The demo starts three servers, two clients and a console on ports 9200--9202.
If those ports are occupied, it stops rather than reusing an existing server.
To choose another base port:

```bash
PORT=9300 bash demo/bogus_phase1.sh bogus
PORT=9300 bash demo/bogus_phase1.sh fixed
```

The script waits for log events with timeouts instead of advancing after fixed
sleeps. It restarts the console after S1's crash to avoid reusing a connection
still reconnecting to the previous process. Maven supplies the dependency
classpath, and compiled classes and FIFOs are staged on the Linux filesystem.

Only processes started by this invocation are terminated on exit. Existing manual
sessions are left running. A timeout, unexpected process exit or failed check
returns a nonzero exit code and identifies the log to inspect. A timeout alone
is not treated as evidence of a safety violation.

`bogus_phase1.sh` defines the scenario and assertions; `lab.sh` provides process
startup, terminal input, waiting and cleanup.

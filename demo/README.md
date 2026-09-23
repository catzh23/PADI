# Demos: showing the baseline is bogus

Project Step 1 asks students to *"implement debug modes required to show that the
baseline is bogus"*. This directory holds a scripted, self-checking reproduction.

```
mvn clean install              # once, from the repo root
./demo/bogus_phase1.sh bogus   # baseline phase 1  -> EXPECT divergence
./demo/bogus_phase1.sh fixed   # real phase 1      -> EXPECT agreement
```

Both exit 0 when the expected outcome is observed, so the pair works as a regression
test. `bogus` selects the old processor with `-Ddidatrade.phase1=bogus`;
`PhaseOneBogusProcessor` is kept in the tree for exactly this reason.

Per-process logs land in `demo/run/` (gitignored).

## What is bogus

`util/src/main/java/didatrade/util/PhaseOneBogusProcessor.java` is the whole problem:

```java
this.accepted = true;                     // line 19: set once, never reassigned
public boolean onNext(..., last_response) {
  this.maxballot = last_response.getMaxballot();
  this.value     = last_response.getValue();
  this.valballot = last_response.getValballot();
  return true;                            // line 50: "done" after the FIRST reply
}
```

Three separate defects:

1. It never reads `last_response.getAccepted()`, so a leader whose ballot was
   rejected proceeds to phase 2 anyway.
2. It returns `true` on the first reply, so phase 1 never reads a **quorum**.
3. It overwrites `value`/`valballot` with whatever arrived last, instead of keeping
   the value carried by the reply with the **highest `valballot`**.

Defect 2 + 3 destroy the safety property of Paxos: a value that an earlier ballot
already got accepted by a quorum can be silently forgotten, and the slot re-decided
with something else.

`MainLoop.java:78-79` still carries the commented-out call to the class you are
meant to write in its place:

```java
// PhaseOneResponseProcessor phase_one_processor = new PhaseOneResponseProcessor(...)
```

## What the script does

Schedule A, 3 servers, ballot *b* has leader `b % 3`.

| step | action | why |
|---|---|---|
| 1 | client 1 runs `sell 0 50` | becomes log slot 0, decided by all three at ballot 0, value `101` |
| 2 | `debug crash 1`, then relaunch S1 | S1 returns with an **empty** `PaxosLog` — the baseline has no state transfer and no stable storage |
| 3 | `debug slow-mode-on 0`, `debug slow-mode-on 2` | phase 1 stops at the first reply, so this makes the restarted, empty S1 win that race deterministically |
| 4 | `ballot 1 1` | S1 becomes leader of ballot 1 |
| 5 | client 2 runs `sell 3 50` | S1 proposes it for *its* slot 0 — the slot already holding `101` |

Steps 2 and 3 are exactly the `crash` and `slow-mode-on` debug modes the project
description requires; they are what makes the race controllable instead of luck.

## Result

```
command executed at log slot 0:
  S0 -> 101
  S1 -> 102
  S2 -> 101

S1 phase 1 for slot 0:
  Paxos phase 1 ended with aborted = false and read ballot = -1 and value 102

state machine effects:
  S0: in sell for user 0;
  S1: in sell for user 3;
  S2: in sell for user 0;
```

`read ballot = -1` is `getValballot()`. S1 consulted exactly one acceptor — itself,
freshly restarted and empty — concluded that nothing had ever been accepted for
slot 0, and proposed its own value.

A correct phase 1 waits for a quorum. With 3 acceptors any quorum of 2 must contain
S0 or S2, both of which hold `valballot = 0, value = 101`, so the leader would have
been **forced** to re-propose `101` and slot 0 would have survived the leader change.

Consequences visible in the logs:

- S0 and S2 applied `sell 0 50` at slot 0; S1 applied `sell 3 50` at slot 0. The
  replicated state machines have diverged and never reconverge.
- Client 2 received a success reply for `sell 3 50` even though the majority never
  executed it.
- In `demo/run/s0.log`, look for `Paxos learner for instance 0 : resetting` under
  `timestamp 1`: S0 *accepted* the ballot-1 write to slot 0 and rewrote its own log
  entry to `102`, while its state machine had already executed `101`. Nothing in
  `DidaTradePaxosServiceImpl.phasetwo` checks `entry.decided` before overwriting,
  so even the surviving majority ends up with a log that contradicts its own state.

## After the fix

`./demo/bogus_phase1.sh fixed` on the same scenario:

```
command executed at log slot 0:
  S0 -> 101
  S1 -> 101
  S2 -> 101

S1 phase 1 for slot 0:
  Paxos phase 1 ended with aborted = false and read ballot = 0 and value 101
```

`read ballot` went from `-1` to `0`: S1 waited for a quorum, one of which had to be
S0 or S2, found `valballot = 0, value = 101`, and was forced to re-propose it
instead of its own request. Safety holds.

**What this then exposes.** S1's log now agrees, but S1 never *executes* slot 0 — it
loops on `Record not available!` (`MainLoop.java:173`). It learned *which* request
id was decided but never received the request itself, because client 1 sent it
before S1 restarted. Consensus on the order is not enough when the value is only a
request id; a replica also needs the command. That is the request-forwarding /
state-transfer hole, and it belongs to the `MainLoop.java:176` row of the issues
table, not to phase 1.

## Variant worth showing in the discussion

Defect 1 in isolation: give one acceptor a higher ballot than the current leader
(`ballot 1 1` while S0 still believes it leads ballot 0). S0's acceptors reply
`accepted = false`, and S0's log still prints `Paxos phase 1 ended with aborted =
false` — phase 1 is purely decorative. Phase 2 happens to catch it here, because
`PhaseTwoResponseProcessor` *does* check `getAccepted()`. That contrast between the
two processors is the clearest single illustration of what is missing.

## Reproducibility

Deterministic in practice — the slow-mode delay (500–4000 ms, `DebugInterceptor`)
is far larger than the loopback RPC latency S1 races against. The script exits 0 on
violation, 1 if a run happens not to reproduce.

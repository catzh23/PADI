# Step 5: dynamic membership and activation

Build and run from the repository root, in WSL/Linux:

```bash
mvn clean install
python3 validation/step5/run.py
```

The tests use seven replicas with Schedule C. The default base port is 19570;
use `--port` to change it. Controlled checks also use base+20 through base+56.
Logs and `results.json` are saved under `validation/step5/runs/`.

## Protocol used

Ballots 0 and 1 retain the existing fixed-membership behavior. Manual changes to ballots 2, 3 and 4
use preparation followed by explicit activation. This also covers a later ballot
whose acceptor list is unchanged but whose predecessor never finished transferring.

1. The console sends `ballot b leader` with its last completed configuration.
   Before the first transfer, configuration 0 is the default reference: ballots
   0 and 1 have the same acceptors. Their replies still report the actual accepted
   ballot of every slot, so Fast and classic recovery use the recorded history.
2. The leader prepares a quorum of that reference configuration. If ballot 3 or
   4 still refers to configuration 0 or 1, it also needs a quorum of S2-S6:
   an unfinished intermediate transfer may already have decided some slots there.
   It needs three distinct promises in each group, not three from their union.
   A shared acceptor counts once in each group. The highest accepted ballot is
   considered per slot across all collected replies. For a transfer, the leader
   asks for the entire accepted log, including slots it has already executed.
3. It preserves the recovered values in the new acceptor group using classic
   phase 2, even when the new ballot will later use Fast Paxos. Empty holes in
   the recovered prefix are filled with no-ops. Request bodies travel with the
   values so lagging learners can execute them without the original client RPC.
4. Only after every transfer proposal has a positive quorum does the leader
   report `completed = b` to the console. A local decision or a sent RPC alone
   does not complete the transfer. An empty history completes after phase 1.
5. `activate b leader` permits new client proposals and, in a Fast ballot, `any`.
   Premature activation and activation of a superseded ballot are rejected.

The same console must remain running during the experiment: it is the master
and retains the last completed reference. This implementation does not add
persistent storage or recovery of a restarted master.

A collision in Fast ballot 2 starts ballot 3 automatically. Fast votes in ballot 2
are only sent after activation, so configuration 2 is a valid recovery reference.
The acceptor group does not change: S4 prepares S2-S6, preserves the recovered
values with phase 2 quorums, and then resumes new requests without console commands.
This does not bypass the initial transfer or activate an unfinished manual preparation.
If the scheduled leader is unavailable, a manual ballot change is still needed.
Schedule C ends at ballot 4; ballot 5 is rejected.

## Manual reproduction

Start seven server terminals, using IDs 0 through 6:

```bash
cd server
mvn exec:java -Dexec.args="8080 0 C"
```

In separate terminals, from the repository root:

```bash
cd app
mvn exec:java -Dexec.args="1 localhost 8080 C"
```

```bash
cd console
mvn exec:java -Dexec.args="localhost 8080 C"
```

In the console, enter `ballot 1 1`. In the client, enter `sell 0 50` and
`buy 1 50`, waiting for each response. The console should report ballot 1 completed.

Then enter `ballot 2 3` in the console. Wait for:

```text
reply received to new ballot request for ballot = 2 with completed = 2
```

Submit `sell 2 50` in the client. It waits: the new configuration is prepared
but has not been activated. Enter `activate 2 3` in the console. The pending
request can now finish. Then `buy 3 50` and `balance 2` should give balance 15.

You can now crash S0 and S1 with `debug crash 0` and `debug crash 1`. Enter
`ballot 3 4`, wait for completed 3, then `activate 3 4`. Recovery uses the new
group and no longer depends on S0/S1. The final configuration is similarly
started with `ballot 4 5`, followed by `activate 4 5` after preparation completes.

## What the tests check

- A missing old quorum prevents preparation and activation.
- A missing new quorum prevents completion of the transfer.
- Already executed slots are still transferred to the new acceptors.
- A learner without the original requests reconstructs and executes the history.
- A late client RPC reuses a transferred request's response instead of executing again.
- Empty histories and holes in the recovered prefix are handled.
- New requests and `any` are held until activation.
- Recovery continues after removing S0/S1, using the completed new configuration.
- A preparation interrupted in ballot 2 is recovered by ballot 3 from both groups.
- A value already decided during an unfinished transfer is preserved even if
  the old group's replies alone would select a different value.
- Neither an old-only quorum nor a new-only quorum completes this recovery;
  duplicates do not count twice, and shared acceptors can satisfy both groups.
- Stale activation, old-ballot writes and writes to removed acceptors are rejected.
- A collision in activated ballot 2 recovers automatically without S0/S1 or console
  commands, preserves an already chosen slot, and waits for phase 2 quorums before
  serving new requests.

The tests exercise the real services and leader loop. Controlled checks construct
specific accepted histories before starting RPC servers; live scenarios use the
normal server, client and console programs. Passing them is evidence for these
scenarios, not a proof of all possible executions.

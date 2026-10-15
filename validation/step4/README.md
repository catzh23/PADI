# Validation of Step 4: causally ordered balance

```bash
python3 validation/step4/run.py
```

Three replicas (schedule A), one client and the console. The console starts
classic ballot 1 before the writes, to test causal reads independently of Fast
Paxos and allow writes with one frozen replica. The script checks that:

1. after `sell 0 50` and `buy 1 50`, `balance 0` returns 15 and no replica puts it
   in the Paxos log;
2. with the leader S1 frozen, `balance 1` is still answered (5) by S0/S2;
3. S2 is frozen while `sell 2 50` and `buy 3 50` are decided. After un-freeze,
   `balance 2` reaches S2 with `minindex 4`; S2 waits until it has executed those
   writes and answers 15. Neither the client nor S2 ever shows the old balance (10).

Before the live scenario, `CausalChecks.java` pauses a write after changing the
balance and checks that a concurrent read waits for the index to be published.
It also checks that the write's index is set before its response becomes available.

Uses the shared harness `../lab.py`, the same base port as Step 1 (9200, change it
with `--port`), and keeps logs under `validation/step4/runs/`.
Build first with `mvn clean install`.

#!/usr/bin/env python3
"""Step 1: the baseline phase 1 is unsafe, the corrected one is not.

  python3 validation/step1/run.py bogus   # baseline -> slot 0 diverges (expected)
  python3 validation/step1/run.py fixed   # correct  -> old decision is preserved
"""
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lab

p = lab.parser()
p.add_argument('mode', choices=['bogus', 'fixed'])
args = p.parse_args()
lab.init(Path(__file__).parent, args.mode, args.port)
bogus = args.mode == 'bogus'


def slot0(l, i):
    m = re.search(r'Log entry with number 0 has been decided with command id = (\d+)', l.log('s'+str(i)))
    return m.group(1) if m else None


def scenario():
    with lab.Lab(args.mode, args.mode) as l:
        print('[1] Start three replicas and decide request 101 at slot 0.', flush=True)
        l.boot()
        l.send('app1', 'sell 0 50')
        for i in range(3):
            l.wait_log('s'+str(i), 'Log entry with number 0 has been decided with command id = 101')

        print('[2] Crash and restart S1 with an empty log.', flush=True)
        l.crash(1)
        l.server(1)
        # A new console avoids racing the old channel's reconnect backoff after the crash.
        l.stop('console')
        l.console_start()

        print('[3] Freeze S0/S2: only the empty S1 can answer prepare immediately.', flush=True)
        l.debug('freeze', 0)
        l.debug('freeze', 2)
        l.send('console', 'ballot 1 1')
        l.wait_log('s1', 'new ballot = 1')
        l.wait_log('s1', 'Going to run paxos phase 1 for ballot 1')
        if bogus:
            l.wait_log('s1', 'using the BOGUS processor')
            l.wait_log('s1', r'Multi-prepare completed for ballot 1; recovered slots = \[\]')

        print('[4] Send request 102. The old decision at slot 0 must remain 101.', flush=True)
        l.send('app2', 'sell 3 50')
        l.wait_log('s1', 'Adding sell request with reqid 102 to pending')
        if bogus:
            l.wait_log('s1', 'Sending phase 2 for slot 0 in ballot 1, value 102')
        else:
            lab.record('fixed: prepare waits for a majority while S0/S2 are frozen',
                       'Multi-prepare completed for ballot 1' not in l.log('s1'))

        print('[5] Unfreeze the surviving acceptors and inspect the decision.', flush=True)
        l.debug('un-freeze', 0)
        l.debug('un-freeze', 2)
        if bogus:
            l.wait_log('s1', 'Log entry with number 0 has been decided with command id = 102')
            chosen = [slot0(l, i) for i in range(3)]
            lab.record('bogus: baseline defect reproduced (slot 0 diverges)',
                       chosen == ['101', '102', '101'], 'S0/S1/S2 = '+'/'.join(map(str, chosen)))
        else:
            l.wait_log('s1', 'Multi-prepare completed for ballot 1')
            l.wait_log('s1', 'Sending phase 2 for slot 0 in ballot 1, value 101')
            l.wait_log('s1', 'Phase 2 chose slot 0 in ballot 1|Paxos learner decided slot 0 in ballot 1, value 101')
            lab.record('fixed: S1 recovered 101 and never proposed 102 for slot 0',
                       not re.search('Sending phase 2 for slot 0 in ballot 1, value 102|'
                                     'Log entry with number 0 has been decided with command id = 102',
                                     l.log('s1')))
            # Consensus only: S1 lost the body of request 101 in the crash and cannot execute it.


lab.run([(args.mode, scenario)])

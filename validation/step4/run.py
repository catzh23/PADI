#!/usr/bin/env python3
"""Step 4: balance is read locally without Paxos, and never shows older state than
the client has already seen.

  python3 validation/step4/run.py
"""
from pathlib import Path
import sys
import subprocess
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lab

args = lab.parser().parse_args()
OUT = lab.init(Path(__file__).parent, 'causal', args.port)


def concurrent_checks():
    source = Path(__file__).parent/'CausalChecks.java'
    subprocess.run(['javac', '-proc:none', '-cp', lab.CP, '-d', str(OUT), str(source)], check=True)
    result = subprocess.run(['java', '-cp', str(OUT)+':'+lab.CP,
                             'didatrade.server.CausalChecks'],
                            capture_output=True, text=True, timeout=30)
    (OUT/'concurrency.log').write_text(result.stdout+result.stderr)
    print(result.stdout, flush=True)
    lab.record('causal concurrency checks', result.returncode == 0)


def scenario():
    # Client 1 request ids are <command number>01: sell 101, buy 201, balance 301, ...
    with lab.Lab('causal', clients=(1,)) as l:
        l.boot()
        # Isolate causal reads from Fast Paxos: ballot 1 uses a classic quorum.
        l.send('console', 'ballot 1 1')
        l.wait_log('s1', 'Multi-prepare completed for ballot 1')

        l.send('app1', 'sell 0 50')
        l.send('app1', 'buy 1 50')
        for i in range(3):
            l.wait_log('s'+str(i), 'Setting response for command with id = 201 with result = true')
        l.send('app1', 'balance 0')
        l.wait_log('app1', 'User 0 balance is 15')
        time.sleep(1)
        lab.record('balance 0 = 15, answered without a Paxos log entry',
                   not any('Setting response for command with id = 301 ' in l.log('s'+str(i)) for i in range(3)))

        # The leader is frozen, so no write can be decided, but balance does not need Paxos.
        l.debug('freeze', 1)
        l.send('app1', 'balance 1')
        l.wait_log('app1', 'User 1 balance is 5', 10)
        lab.record('balance 1 = 5 while the leader S1 is frozen',
                   'Balance for reqid 401' not in l.log('s1'))
        l.debug('un-freeze', 1)

        # S2 misses two writes; right after un-freeze it is behind but must not answer old state.
        # Classic ballot 1 lets S0/S1 decide writes while S2 is frozen.
        l.debug('freeze', 2)
        l.send('app1', 'sell 2 50')
        l.send('app1', 'buy 3 50')
        for i in (0, 1):
            l.wait_log('s'+str(i), 'Setting response for command with id = 601 with result = true')
        l.debug('un-freeze', 2)
        l.send('app1', 'balance 2')
        l.wait_log('app1', 'User 2 balance is 15')
        l.wait_log('s2', 'reqid 701 userid 2 minindex 4')
        l.wait_log('s2', 'Balance for reqid 701 is 15 at index 4')
        lab.record('balance 2 = 15; lagging S2 waited for index 4 and nobody saw 10',
                   'User 2 balance is 10' not in l.log('app1')
                   and 'Balance for reqid 701 is 10' not in l.log('s2'))


lab.run([('concurrency', concurrent_checks), ('causal', scenario)])

#!/usr/bin/env python3
"""Seven real replicas: transfer, activation, and an interrupted preparation."""
from pathlib import Path
import socket
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lab

args = lab.parser(19570).parse_args()
OUT = lab.init(Path(__file__).parent, 'membership', args.port)


class ScheduleCLab(lab.Lab):
    def boot(self):
        for port in range(lab.PORT + 3, lab.PORT + 7):
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(('127.0.0.1', port))
        for i in range(7):
            self.start('s'+str(i), 'didatrade.server.DidaTradeServer', [lab.PORT, i, 'C'])
            self.wait_log('s'+str(i), 'Server started')
        self.start('app1', 'didatrade.app.DidaTradeApp', [1, 'localhost', lab.PORT, 'C'])
        self.wait_log('app1', 'app>')
        self.start('console', 'didatrade.console.Console', ['localhost', lab.PORT, 'C'])
        self.wait_log('console', 'console>')

    def initial_writes(self):
        self.send('console', 'ballot 1 1')
        self.wait_log('s1', 'Multi-prepare completed for ballot 1')
        self.send('app1', 'sell 0 50')
        self.send('app1', 'buy 1 50')
        for i in range(7):
            self.wait_log('s'+str(i), 'Setting response for command with id = 201 with result = true')
        self.wait_log('console', 'ballot = 1 with completed = 1')

    def prepared(self, ballot):
        self.wait_log('console', f'ballot = {ballot} with completed = {ballot}')


def transfer_and_activate():
    with ScheduleCLab('transfer', clients=(1,)) as l:
        l.boot()
        l.initial_writes()
        # Only two old acceptors can answer: preparation must not complete.
        for i in (0, 1, 2): l.debug('freeze', i)
        l.send('console', 'ballot 2 3')
        l.wait_log('s3', 'Recovering ballot 2 from configuration 1')
        l.send('console', 'activate 2 3')
        l.wait_log('console', 'ballot = 2 with ack = false')
        lab.record('activation rejected before the old quorum is recovered',
                   'Preparation complete for ballot 2' not in l.log('s3'))
        for i in (0, 1, 2): l.debug('un-freeze', i)
        l.prepared(2)
        lab.record('already executed slots are transferred again to the new acceptors',
                   all(f'Transferred slot {s} in ballot 2' in l.log('s3') for s in (0, 1)))
        l.send('app1', 'sell 2 50')
        time.sleep(1)
        lab.record('no new request or any before activate',
                   not any(l.executed(i, 301) for i in range(7))
                   and "Sending 'any' from slot 2 in ballot 2" not in l.log('s3'))
        l.send('console', 'activate 2 3')
        l.wait_log('console', 'ballot = 2 with ack = true')
        l.wait_log('s3', 'Setting response for command with id = 301 with result = true')
        lab.record('Fast ballot resumes after activate', True)

        # The next recovery must work without the two removed acceptors.
        l.crash(0)
        l.crash(1)
        l.send('console', 'ballot 3 4')
        l.prepared(3)
        lab.record('next preparation uses completed configuration 2 without S0/S1',
                   'Recovering ballot 3 from configuration 2' in l.log('s4'))
        l.send('console', 'activate 3 4')
        l.wait_log('console', 'ballot = 3 with ack = true')
        l.send('app1', 'buy 3 50')
        for i in range(2, 7):
            l.wait_log('s'+str(i), 'Setting response for command with id = 401 with result = true')
        l.send('app1', 'balance 2')
        l.wait_log('app1', 'User 2 balance is 15')
        lab.record('new group continues trading and causal balance returns 15', True)
        l.send('console', 'ballot 4 5')
        l.prepared(4)
        l.send('console', 'activate 4 5')
        l.wait_log('console', 'ballot = 4 with ack = true')
        l.send('app1', 'sell 4 50')
        l.send('app1', 'buy 5 50')
        for i in range(2, 7):
            l.wait_log('s'+str(i), 'Setting response for command with id = 701 with result = true')
        l.send('app1', 'balance 4')
        l.wait_log('app1', 'User 4 balance is 15')
        lab.record('final configuration 4 recovers configuration 3 and serves new requests',
                   'Recovering ballot 4 from configuration 3' in l.log('s5'))


def interrupted_preparation():
    with ScheduleCLab('interrupted', clients=(1,)) as l:
        l.boot()
        l.initial_writes()
        # Old quorum remains reachable, but only S2/S4 can accept the transfer.
        for i in (3, 5, 6): l.debug('freeze', i)
        l.send('console', 'ballot 2 3')
        l.wait_log('s3', 'Sending phase 2 for slot 0 in ballot 2')
        l.send('console', 'activate 2 3')
        l.wait_log('console', 'ballot = 2 with ack = false')
        lab.record('two new acceptors are not enough to complete transfer',
                   'Preparation complete for ballot 2' not in l.log('s3'))
        l.send('console', 'ballot 3 4')
        l.wait_log('s4', 'Recovering ballot 3 from configuration 1')
        for i in (3, 5, 6): l.debug('un-freeze', i)
        l.prepared(3)
        lab.record('ballot 3 skips unfinished ballot 2 and recovers the old group', True)
        l.send('app1', 'sell 2 50')
        time.sleep(1)
        lab.record('same acceptor list does not bypass activation after interruption',
                   not any(l.executed(i, 301) for i in range(7)))
        l.send('console', 'activate 3 4')
        l.wait_log('console', 'ballot = 3 with ack = true')
        l.wait_log('s4', 'Setting response for command with id = 301 with result = true')
        lab.record('interrupted transfer recovers and resumes after activation', True)


def controlled_checks():
    source = Path(__file__).parent/'MembershipChecks.java'
    subprocess.run(['javac', '-proc:none', '-cp', lab.CP, '-d', str(OUT), str(source)], check=True)
    result = subprocess.run(['java', '-cp', str(OUT)+':'+lab.CP,
                             'didatrade.server.MembershipChecks', str(lab.PORT + 20)],
                            capture_output=True, text=True, timeout=60)
    (OUT/'controlled.log').write_text(result.stdout+result.stderr)
    print('\n'.join(line for line in result.stdout.splitlines() if line.startswith('PASS ')), flush=True)
    if result.returncode: print(result.stderr, flush=True)
    lab.record('controlled membership checks', result.returncode == 0)


lab.run([('controlled', controlled_checks), ('transfer', transfer_and_activate),
         ('interrupted', interrupted_preparation)])

#!/usr/bin/env python3
"""Run in WSL after mvn clean install. Owns only the subprocesses it starts.
Logs are retained in a new timestamped directory. No application files are edited.
"""
import argparse
import atexit
import json
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument('scenario', choices=['processors', 'multiprepare', 'pipeline', 'bogus', 'fixed', 'debug', 'all'])
parser.add_argument('--port', type=int, default=19340)
args = parser.parse_args()
OUT = HERE / 'runs' / (time.strftime('%Y%m%d-%H%M%S') + '-' + args.scenario)
OUT.mkdir(parents=True, exist_ok=False)
modules = ['server', 'app', 'console', 'core', 'configs', 'util', 'contract']
# Ask Maven for the actual dependency graph, never mix every JAR in ~/.m2.
with open(OUT/'classpath-build.log', 'w') as log:
    subprocess.run(['mvn','-B','-f',str(ROOT/'server/pom.xml'),
                    'dependency:build-classpath','-Dmdep.outputFile='+str(OUT/'classpath.txt')],
                   stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
# Loading hundreds of generated classes directly from /mnt/c is very slow on
# some Windows/WSL installations. Stage only compiled output on the Linux FS.
staging = tempfile.TemporaryDirectory(prefix='didatrade-part1-')
atexit.register(staging.cleanup)
class_dirs = []
for module in modules:
    dest = Path(staging.name)/module
    shutil.copytree(ROOT/module/'target/classes', dest)
    class_dirs.append(str(dest))
CP = ':'.join(class_dirs
              + [(OUT/'classpath.txt').read_text().strip()])
results = []

def record(name, ok, detail=''):
    item = {'name': name, 'pass': bool(ok), 'detail': detail}
    results.append(item)
    print(('PASS ' if ok else 'FAIL ') + name + (' | ' + detail if detail else ''), flush=True)

def wait_for(fn, timeout=45):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if fn(): return True
        time.sleep(.1)
    return False

class Lab:
    def __init__(self, name, mode='fixed'):
        self.path = OUT/name
        self.path.mkdir()
        self.mode = mode
        self.procs = {}
        self.handles = []
        for port in range(args.port, args.port+3):
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(('127.0.0.1', port))
                sock.listen(1)
    def start(self, name, cls, argv, flags=()):
        old = self.procs.get(name)
        if old and old.poll() is None: raise RuntimeError('Process still running: '+name)
        path = self.path/(name+'.log')
        if path.exists(): path.rename(self.path/(name+'-before-restart.log'))
        handle = open(path, 'w')
        self.handles.append(handle)
        proc = subprocess.Popen(['java', *flags, '-cp', CP, cls, *map(str,argv)],
                stdin=subprocess.PIPE, stdout=handle, stderr=subprocess.STDOUT, text=True)
        self.procs[name] = proc
        return proc
    def server(self, i):
        self.start('s'+str(i), 'didatrade.server.DidaTradeServer', [args.port,i,'A'],
                   ['-Ddidatrade.phase1=bogus'] if self.mode == 'bogus' else [])
        if not wait_for(lambda: 'Server started' in self.log('s'+str(i))):
            raise RuntimeError('Server failed to start; inspect '+str(self.path))
    def boot(self):
        for i in range(3): self.server(i)
        for i in (1,2):
            self.start('app'+str(i), 'didatrade.app.DidaTradeApp', [i,'localhost',args.port,'A'])
        self.start('console', 'didatrade.console.Console', ['localhost',args.port,'A'])
        if not wait_for(lambda: all('app>' in self.log('app'+str(i)) for i in (1,2))
                        and 'console>' in self.log('console')):
            raise RuntimeError('Client/console startup timeout')
    def send(self, name, command):
        self.procs[name].stdin.write(command+'\n')
        self.procs[name].stdin.flush()
    def log(self, name):
        return (self.path/(name+'.log')).read_text(errors='replace')
    def debug(self, mode, i):
        self.send('console', f'debug {mode} {i}')
        if not wait_for(lambda: 'Setting debug mode to = '+mode in self.log('s'+str(i))):
            raise RuntimeError('Debug command did not arrive: '+mode)
    def executed(self, i, req):
        return f'Setting response for command with id = {req} with result = true' in self.log('s'+str(i))
    def close(self):
        for p in self.procs.values():
            if p.poll() is None: p.terminate()
        for p in self.procs.values():
            try: p.wait(timeout=3)
            except subprocess.TimeoutExpired: p.kill(); p.wait()
            if p.stdin: p.stdin.close()
        for h in self.handles: h.close()

def processors():
    subprocess.run(['javac','-proc:none','-cp',CP,'-d',str(OUT),str(HERE/'ProcessorChecks.java')],check=True)
    result = subprocess.run(['java','-cp',str(OUT)+':'+CP,'ProcessorChecks'],capture_output=True,text=True,timeout=15)
    (OUT/'processors.log').write_text(result.stdout+result.stderr)
    print(result.stdout,flush=True)
    record('processor checks', result.returncode == 0, 'See processors.log; failures are findings, not harness success.')

def multiprepare():
    subprocess.run(['javac','-proc:none','-cp',CP,'-d',str(OUT),str(HERE/'MultiPrepareChecks.java')],check=True)
    result = subprocess.run(['java','-cp',str(OUT)+':'+CP,'didatrade.server.MultiPrepareChecks'],
                            capture_output=True,text=True,timeout=90)
    (OUT/'multiprepare.log').write_text(result.stdout+result.stderr)
    for line in result.stdout.splitlines():
        if line.startswith(('PASS ', 'FAIL ', 'Failed checks:')): print(line,flush=True)
    record('multi-prepare checks', result.returncode == 0, 'See multiprepare.log')

def pipeline():
    subprocess.run(['javac','-proc:none','-cp',CP,'-d',str(OUT),str(HERE/'PipelineChecks.java')],check=True)
    result = subprocess.run(['java','-cp',str(OUT)+':'+CP,'didatrade.server.PipelineChecks'],
                            capture_output=True,text=True,timeout=120)
    (OUT/'pipeline.log').write_text(result.stdout+result.stderr)
    for line in result.stdout.splitlines():
        if line.startswith(('PASS ', 'FAIL ', 'Failed checks:')): print(line,flush=True)
    record('pipeline checks', result.returncode == 0, 'See pipeline.log')

def recovery(mode):
    lab = Lab(mode, mode)
    try:
        lab.boot()
        lab.send('app1','sell 0 50')
        if not wait_for(lambda: all(lab.executed(i,101) for i in range(3))):
            raise RuntimeError('Initial sell did not execute on all replicas')
        lab.debug('crash',1)
        if not wait_for(lambda: lab.procs['s1'].poll() is not None): raise RuntimeError('Crash did not exit')
        lab.server(1)
        lab.debug('slow-mode-on',0)
        lab.debug('slow-mode-on',2)
        lab.send('console','ballot 1 1')
        if not wait_for(lambda: 'new ballot = 1' in lab.log('s1')): raise RuntimeError('Ballot not delivered')
        lab.send('app2','sell 3 50')
        if not wait_for(lambda: 'Log entry with number 0 has been decided' in lab.log('s1'),40):
            raise RuntimeError('S1 did not decide slot 0 within 40s')
        time.sleep(2)
        chosen = []
        for i in range(3):
            m = re.search(r'Log entry with number 0 has been decided with command id = (\d+)',lab.log('s'+str(i)))
            chosen.append(m.group(1) if m else None)
        record(mode+': same chosen request at slot 0', chosen == ['101']*3, str(chosen))
        record(mode+': restarted S1 actually executes original request', lab.executed(1,101),
               'Record not available='+str('Record not available!' in lab.log('s1')))
        if mode == 'bogus':
            print('Bogus mode is EXPECTED to fail agreement; this reproduces the old bug.',flush=True)
    finally: lab.close()

def debug():
    lab = Lab('debug')
    try:
        lab.boot()
        lab.send('app1','sell 0 50')
        record('normal: sell replicated',wait_for(lambda: all(lab.executed(i,101) for i in range(3))))
        lab.send('app1','buy 1 50')
        record('normal: buy replicated',wait_for(lambda: all(lab.executed(i,201) for i in range(3))))
        lab.send('app1','balance 0')
        record('normal: seller balance 15',wait_for(lambda: 'User 0 balance is 15' in lab.log('app1')))
        lab.send('app1','balance 1')
        record('normal: buyer balance 5',wait_for(lambda: 'User 1 balance is 5' in lab.log('app1')))
        lab.debug('freeze',0)
        lab.send('app1','sell 2 50')
        time.sleep(2)
        record('freeze: leader does not execute queued request',not lab.executed(0,501))
        lab.debug('un-freeze',0)
        record('un-freeze: queued request executes without resending',wait_for(lambda: all(lab.executed(i,501) for i in range(3))))
        lab.debug('slow-mode-on',0)
        start=time.monotonic()
        lab.send('app1','balance 2')
        done=wait_for(lambda: all(lab.executed(i,601) for i in range(3)),35)
        elapsed=time.monotonic()-start
        record('slow mode: request completes with delay',done and elapsed >= .5,f'{elapsed:.2f}s')
        lab.debug('slow-mode-off',0)
        start=time.monotonic()
        lab.send('app1','balance 2')
        done=wait_for(lambda: all(lab.executed(i,701) for i in range(3)))
        record('slow mode off: next request completes',done,f'{time.monotonic()-start:.2f}s')
        lab.debug('crash',2)
        record('crash: process exits',wait_for(lambda: lab.procs['s2'].poll() is not None))
        lab.send('app1','sell 3 50')
        record('one crash: surviving majority processes request',wait_for(lambda: lab.executed(0,801) and lab.executed(1,801)))
    finally: lab.close()

try:
    for scenario in (['processors','multiprepare','pipeline','debug','bogus','fixed'] if args.scenario == 'all' else [args.scenario]):
        print('\nSCENARIO '+scenario,flush=True)
        try:
            if scenario == 'processors': processors()
            elif scenario == 'multiprepare': multiprepare()
            elif scenario == 'pipeline': pipeline()
            elif scenario == 'debug': debug()
            else: recovery(scenario)
        except Exception as exc:
            record(scenario+': harness/scenario completed',False,repr(exc))
finally:
    (OUT/'results.json').write_text(json.dumps(results,indent=2))
    print('\nLogs: '+str(OUT),flush=True)
raise SystemExit(1 if any(not r['pass'] for r in results) else 0)

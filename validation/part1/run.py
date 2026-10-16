#!/usr/bin/env python3
"""Run after mvn clean install. Owns only the subprocesses it starts.
Logs are retained in a new timestamped directory. No application files are edited.
The Step 1 bogus/fixed scenario lives in validation/step1/run.py.
"""
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lab
from lab import Lab, record, wait_for

HERE = Path(__file__).resolve().parent
parser = lab.parser(19340)
parser.add_argument('scenario', choices=['processors', 'multiprepare', 'pipeline', 'debug', 'all'])
args = parser.parse_args()
OUT = lab.init(HERE, args.scenario, args.port)
CP = lab.CP

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
        lab.send('app1','buy 2 50')  # a write: balance no longer goes through Paxos (Step 4)
        done=wait_for(lambda: all(lab.executed(i,601) for i in range(3)),35)
        elapsed=time.monotonic()-start
        record('slow mode: request completes with delay',done and elapsed >= .5,f'{elapsed:.2f}s')
        lab.debug('slow-mode-off',0)
        start=time.monotonic()
        lab.send('app1','buy 3 50')
        done=wait_for(lambda: all(lab.executed(i,701) for i in range(3)))
        record('slow mode off: next request completes',done,f'{time.monotonic()-start:.2f}s')
        # Schedule A's Fast ballot 0 needs all three acceptors. Use a classic
        # ballot to check that two surviving replicas can still make progress.
        lab.send('console', 'ballot 1 1')
        lab.wait_log('s1', 'Multi-prepare completed for ballot 1')
        lab.debug('crash',2)
        record('crash: process exits',wait_for(lambda: lab.procs['s2'].poll() is not None))
        lab.send('app1','sell 3 50')
        record('one crash: surviving majority processes request',wait_for(lambda: lab.executed(0,801) and lab.executed(1,801)))
    finally: lab.close()

SCENARIOS = {'processors': processors, 'multiprepare': multiprepare,
             'pipeline': pipeline, 'debug': debug}
lab.run([(n, SCENARIOS[n]) for n in (SCENARIOS if args.scenario == 'all' else [args.scenario])])

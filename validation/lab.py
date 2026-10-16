"""Shared harness for the live validation scripts in validation/<step>/run.py.

Starts real servers, clients and the console from the compiled classes, drives
them through their terminals and reads their logs. Owns only the subprocesses it
starts. Each invocation keeps its logs in a new timestamped directory under the
calling script's runs/ directory. Build first with mvn clean install.
"""
import argparse
import atexit
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
MODULES = ['server', 'app', 'console', 'core', 'configs', 'util', 'contract']

OUT = None
CP = None
PORT = None
results = []


def parser(default_port=9200):
    p = argparse.ArgumentParser()
    p.add_argument('--port', type=int, default=default_port,
                   help='base port; servers use port, port+1, port+2')
    return p


def init(here, name, port):
    """Create the run directory and the Java classpath. Returns the run directory."""
    global OUT, CP, PORT
    PORT = port
    OUT = Path(here)/'runs'/(time.strftime('%Y%m%d-%H%M%S')+'-'+name)
    OUT.mkdir(parents=True, exist_ok=False)
    if shutil.which('mvn') is None and (ROOT/'dev_env/env.sh').exists():
        # Same fallback as start_servers.sh: take Maven and the JDK from dev_env.
        env = subprocess.run(['bash', '-c', 'source "$0" >/dev/null && env -0', str(ROOT/'dev_env/env.sh')],
                             capture_output=True, text=True, check=True).stdout
        os.environ.update(line.split('=', 1) for line in env.split('\0') if '=' in line)
    # Ask Maven for the actual dependency graph, never mix every JAR in ~/.m2.
    with open(OUT/'classpath-build.log', 'w') as log:
        subprocess.run(['mvn', '-B', '-f', str(ROOT/'server/pom.xml'),
                        'dependency:build-classpath', '-Dmdep.outputFile='+str(OUT/'classpath.txt')],
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
    # Loading hundreds of generated classes directly from /mnt/c is very slow on
    # some Windows/WSL installations. Stage only compiled output on the Linux FS.
    staging = tempfile.TemporaryDirectory(prefix='didatrade-validation-')
    atexit.register(staging.cleanup)
    class_dirs = []
    for module in MODULES:
        if not (ROOT/module/'target/classes').is_dir():
            raise SystemExit('Build first: mvn clean install')
        dest = Path(staging.name)/module
        shutil.copytree(ROOT/module/'target/classes', dest)
        class_dirs.append(str(dest))
    CP = ':'.join(class_dirs + [(OUT/'classpath.txt').read_text().strip()])
    print('Logs: '+str(OUT), flush=True)
    return OUT


def record(name, ok, detail=''):
    results.append({'name': name, 'pass': bool(ok), 'detail': detail})
    print(('PASS ' if ok else 'FAIL ') + name + (' | ' + detail if detail else ''), flush=True)


def wait_for(fn, timeout=45):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if fn(): return True
        time.sleep(.1)
    return False


def run(scenarios):
    """Run (name, function) pairs, write results.json and exit 0 only if all passed."""
    try:
        for name, fn in scenarios:
            print('\nSCENARIO '+name, flush=True)
            try: fn()
            except Exception as exc:
                record(name+': harness/scenario completed', False, repr(exc))
    finally:
        (OUT/'results.json').write_text(json.dumps(results, indent=2))
        print('\nLogs: '+str(OUT), flush=True)
    raise SystemExit(1 if any(not r['pass'] for r in results) else 0)


class Lab:
    """Three servers (schedule A), clients 1..n and the console."""

    def __init__(self, name, mode='fixed', clients=(1, 2)):
        self.path = OUT/name
        self.path.mkdir()
        self.mode = mode
        self.clients = clients
        self.procs = {}
        self.handles = []
        for port in range(PORT, PORT+3):
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try: sock.bind(('127.0.0.1', port))
                except OSError: raise SystemExit(f'Port {port} is in use; choose another --port')

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def start(self, name, cls, argv, flags=()):
        old = self.procs.get(name)
        if old and old.poll() is None: raise RuntimeError('Process still running: '+name)
        path = self.path/(name+'.log')
        if path.exists(): path.rename(self.path/(name+'-before-restart.log'))
        handle = open(path, 'w')
        self.handles.append(handle)
        self.procs[name] = subprocess.Popen(['java', *flags, '-cp', CP, cls, *map(str, argv)],
                                            stdin=subprocess.PIPE, stdout=handle,
                                            stderr=subprocess.STDOUT, text=True)

    def stop(self, name):
        p = self.procs[name]
        if p.poll() is None: p.terminate()
        try: p.wait(timeout=3)
        except subprocess.TimeoutExpired: p.kill(); p.wait()
        if p.stdin: p.stdin.close()

    def server(self, i):
        self.start('s'+str(i), 'didatrade.server.DidaTradeServer', [PORT, i, 'A'],
                   ['-Ddidatrade.phase1=bogus'] if self.mode == 'bogus' else [])
        self.wait_log('s'+str(i), 'Server started')

    def console_start(self):
        self.start('console', 'didatrade.console.Console', ['localhost', PORT, 'A'])
        self.wait_log('console', 'console>')

    def boot(self):
        for i in range(3): self.server(i)
        for i in self.clients:
            self.start('app'+str(i), 'didatrade.app.DidaTradeApp', [i, 'localhost', PORT, 'A'])
            self.wait_log('app'+str(i), 'app>')
        self.console_start()

    def send(self, name, command):
        self.procs[name].stdin.write(command+'\n')
        self.procs[name].stdin.flush()

    def log(self, name):
        path = self.path/(name+'.log')
        # WSL may briefly return ENODATA while a log on /mnt/c is being written.
        for attempt in range(3):
            try:
                return path.read_text(errors='replace') if path.exists() else ''
            except OSError as error:
                if error.errno != 61 or attempt == 2: raise
                time.sleep(.1)

    def wait_log(self, name, pattern, timeout=45):
        """Wait for a regular expression in a process log; raise on timeout or exit."""
        def seen():
            if re.search(pattern, self.log(name)): return True
            if self.procs[name].poll() is not None:
                raise RuntimeError(f'{name} exited; inspect {self.path/name}.log')
            return False
        if not wait_for(seen, timeout):
            raise RuntimeError(f'Timeout waiting for {name}: {pattern}; inspect {self.path/name}.log')

    def debug(self, mode, i):
        self.send('console', f'debug {mode} {i}')
        self.wait_log('s'+str(i), 'Setting debug mode to = '+re.escape(mode))

    def crash(self, i):
        self.debug('crash', i)
        if not wait_for(lambda: self.procs['s'+str(i)].poll() is not None, 15):
            raise RuntimeError(f'S{i} did not crash')

    def executed(self, i, req):
        return f'Setting response for command with id = {req} with result = true' in self.log('s'+str(i))

    def close(self):
        for name in self.procs: self.stop(name)
        for h in self.handles: h.close()

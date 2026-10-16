#!/usr/bin/env python3
"""Isolated local task CLI; no daemon, pi configuration or model calls."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

BASE = Path(__file__).resolve().parent.parent


def configure():
    if sys.version_info < (3, 10):
        raise ValueError('Python >= 3.10 required')
    if os.name != 'posix':
        raise ValueError('POSIX platform required (Linux/macOS); Windows is not supported')
    if not shutil.which('bash'):
        raise ValueError('Missing dependency: bash on PATH; install it yourself, then retry')
    # Capture the sole workspace override before dropping inherited environment.
    chosen = Path(os.environ.get('PLATFORM_WORKSPACE') or 'workspace')
    root = (BASE / chosen).resolve()
    # Read-only preflight: never probe writability by creating files.
    for target in (root, root / 'agents', root / 'home'):
        existing = target
        while not existing.exists():
            existing = existing.parent
        if (not existing.is_dir() or not os.access(existing, os.W_OK | os.X_OK)
                or not existing.stat().st_mode & 0o222):
            raise ValueError('Runtime directory is not writable: ' + str(target)
                             + '; set PLATFORM_WORKSPACE to a writable external directory')
    # PATH is the explicit external executable search path; no shell init is read.
    home = root / 'home'
    env = {'PATH': os.environ.get('PATH', os.defpath), 'HOME': str(home),
           'LANG': 'C.UTF-8', 'PYTHONNOUSERSITE': '1'}
    os.environ.clear()
    os.environ.update(env)
    sys.dont_write_bytecode = True
    return root, 'local'


def ctl(root, *args):
    return subprocess.run([sys.executable, '-I', '-S', '-B',
                           str(BASE / 'agentctl.py'), '--root', str(root), *args],
                          check=True, text=True, capture_output=True)


def demo(root, host):
    # This is a real shell task, NOT a simulated model invocation.
    name = 'shell-' + uuid.uuid4().hex[:12]
    command = "printf '%s\\n' 'PASS: real shell task; no model was called.' > \"$AGENT_HOME/report.md\""
    result = ctl(root, 'create', '--name', name, '--command', command,
                 '--workdir', str(root), '--creator', 'queue/local', '--reaper', 'none',
                 '--host', host, '--created-by-host', host, '--resources', '[]')
    print(result.stdout.strip())
    participant = 'task/' + name
    ctl(root, 'enable', participant, '--by', 'queue/local')
    # Only service this newly registered task. Do not start a global runner loop.
    sys.path.insert(0, str(BASE))
    from runner import Runner
    runner = Runner(str(root), host, [], 0.05)
    adir = root / 'agents/task' / name
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            runner.service(participant)
            state = runner.read_pid(participant)
            if state and state.get('final'):
                report = (adir / 'report.md').read_text(encoding='utf-8')
                if state.get('exitcode') != 0 or 'PASS: real shell task' not in report:
                    raise ValueError('shell task failed: ' + json.dumps(state))
                print(report.strip())
                print('Result: ' + str(adir))
                return
            time.sleep(0.05)
        raise ValueError('shell task timed out')
    finally:
        for child in runner.popen.values():
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest='action', required=True)
    sub.add_parser('doctor')
    sub.add_parser('demo')
    sub.add_parser('ctl').add_argument('args', nargs=argparse.REMAINDER)
    args = ap.parse_args()
    root, host = configure()
    if args.action != 'doctor':
        try:
            (root / 'agents').mkdir(parents=True, exist_ok=True)
            (root / 'home').mkdir(exist_ok=True)
        except OSError as exc:
            raise ValueError('Cannot create runtime directory ' + str(root)
                             + '; set PLATFORM_WORKSPACE to a writable external directory: '
                             + str(exc)) from exc
    if args.action == 'doctor':
        print(json.dumps({'python': sys.version.split()[0], 'workspace': str(root),
                          'host': host, 'bash': shutil.which('bash'),
                          'mode': 'local shell only; no model or pi'}, indent=2))
    elif args.action == 'demo':
        demo(root, host)
    else:
        result = ctl(root, *args.args)
        print(result.stdout, end='')
        print(result.stderr, end='', file=sys.stderr)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        print('portable: ' + str(exc), file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError):
            print(exc.stderr, file=sys.stderr)
        sys.exit(2)

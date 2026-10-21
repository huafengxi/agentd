"""Unwired one-shot agent file client. Never constructs an execution owner.

Only the independent fixed S owner executes/observes workloads. See FILE_BACKEND.md.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time

from .adapter import (Adapter as SyncAdapter, A_ROOT, absolute, inside, require,
                      runtime_id, workdir, agent_ts, fingerprint)
import proto
from runner import Runner, refresh_lock, ts_epoch

S_REVISION = '16d58bce251a7270191ae84b6c70d0a558784bd6'
FINAL = ('succeeded', 'failed', 'canceled', 'lost')


def terminal_phase(run):
    return run['phase'] in FINAL


def load_client(source, *, revision=S_REVISION, dependency='file-backend-dependency.json'):
    """Load only fixed S's file client + schema/IO; no lifecycle/Kernel import."""
    source = absolute(source)
    dependency_path = Path(dependency) if Path(dependency).is_absolute() else Path(__file__).with_name(dependency)
    dep = json.loads(dependency_path.read_text())
    require(dep['revision'] == revision, 'wrong file backend revision')
    require(not list(source.rglob('*.pyc')), 'clean source export required')
    for rel, sha in dep['files'].items():
        p = absolute(source / rel)
        require(p.is_file() and hashlib.sha256(p.read_bytes()).hexdigest() == sha,
                'fixed file backend mismatch: ' + rel)
    if 'runtime' in sys.modules:
        require(Path(sys.modules['runtime'].__file__) == source / 'runtime/__init__.py',
                'ambient runtime refused; use separate client process')
    else:
        spec = importlib.util.spec_from_file_location('runtime', source / 'runtime/__init__.py',
                         submodule_search_locations=[str(source / 'runtime')])
        mod = importlib.util.module_from_spec(spec)
        sys.modules['runtime'] = mod
        spec.loader.exec_module(mod)
    name = '_agent_file_client_' + revision
    if name in sys.modules:
        require(Path(sys.modules[name].__file__) == source / 'runtime/file_owner/protocol.py',
                'client source binding conflict')
        client = sys.modules[name]
    else:
        spec = importlib.util.spec_from_file_location(name, source / 'runtime/file_owner/protocol.py')
        client = importlib.util.module_from_spec(spec)
        sys.modules[name] = client
        spec.loader.exec_module(client)
    return client, dep


class FileAdapter:
    REVISION = S_REVISION
    load_client = staticmethod(load_client)
    EXPERIMENT = 'agent-file-one-shot-fixtures-v1'
    lifecycle = SyncAdapter.lifecycle
    inputs = SyncAdapter.inputs
    initial_workdir = SyncAdapter.initial_workdir

    def __init__(self, *, sandbox_root, workspace, runtime_root, runtime_source, protocol_root, manifest):
        self.sandbox = absolute(sandbox_root)
        self.workspace = absolute(workspace)
        self.runtime_root = absolute(runtime_root)
        self.protocol = absolute(protocol_root)
        source, manifest = absolute(runtime_source), absolute(manifest)
        require(self.sandbox.is_dir(), 'explicit existing sandbox required')
        for p in (self.workspace, self.runtime_root, self.protocol, manifest):
            require(inside(p, self.sandbox), 'mutable input outside sandbox')
        roots = (self.workspace, self.runtime_root, self.protocol, source, A_ROOT)
        for i, p in enumerate(roots):
            for q in roots[i+1:]:
                require(p != q and not inside(p, q) and not inside(q, p), 'overlapping roots')
        require(not any(inside(manifest, p) for p in roots[:3]), 'manifest must be separate')
        require((self.workspace / 'agents').is_dir() and (self.workspace / 'env/host-id').is_file(),
                'explicit synthetic workspace/host-map required')
        self.manifest = json.loads(manifest.read_text())
        require(set(self.manifest) == {'experiment', 'host', 'tasks'} and
                self.manifest['experiment'] == self.EXPERIMENT, 'invalid file manifest')
        self.host = self.manifest['host']
        require(isinstance(self.host, str) and self.host and self.host.replace('-', '').isalnum(), 'invalid host')
        require(type(self.manifest['tasks']) is dict and self.manifest['tasks'], 'finite selection required')
        for pid in self.manifest['tasks']:
            require(proto.parse_participant_id(pid)[0] == 'task', 'task family only')
        self.client, self.dependency = self.load_client(source)
        self.package = 'runtime'  # inputs uses only contract.repository
        # Same stable local inode as original Runner, NOT S's node.lock and NOT agents/run JSON.
        guard = absolute(self.workspace / 'run/locks' / ('agentd.' + self.host + '.lock'))
        guard.parent.mkdir(parents=True, exist_ok=True)
        self.lock = open(guard, 'a+')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.local = self.workspace / 'run/file-adapter'
            self.local.mkdir(parents=True, exist_ok=True)
            binding = {'manifest': self.manifest, 'runtimeRoot': str(self.runtime_root),
                       'protocolRoot': str(self.protocol), 'source': str(source), 'revision': self.REVISION}
            marker = self.local / 'binding.json'
            proto.exclusive_write_json(str(marker), binding)
            require(proto.read_json(str(marker)) == binding, 'workspace binding conflict')
            self.agent = Runner(str(self.workspace), self.host, '', .05)
            self.started_at = proto.now_ts()
            self.progress = {}
            self.verify_binding()  # owner must explicitly initialize; never autostart it
            imports = {n: {'path': str(Path(m.__file__)),
                          'sha256': hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()}
                       for n, m in sys.modules.items() if (n == 'runtime' or n.startswith('runtime.') or n.startswith('_agent_file_client_'))
                       and getattr(m, '__file__', None)}
            proto.atomic_write_json(str(self.local / 'imports.json'), {'revision': self.REVISION, 'imports': imports})
            self.heartbeat()
        except BaseException:
            self.lock.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.lock.close()  # no Runtime, no owner/payload handles, no stop/release

    def heartbeat(self):
        refresh_lock(proto.agentd_lock_path(str(self.workspace), self.host), self.host, self.started_at)

    def verify_binding(self):
        require(self.client.binding(self.protocol) == {'apiVersion': self.client.API,
                'node': self.host, 'runtimeRoot': str(self.runtime_root)}, 'backend binding mismatch')

    def state_path(self, pid):
        return self.local / (runtime_id(self.host, pid) + '.json')

    def read_state(self, pid):
        path = self.state_path(pid)
        if not path.exists():
            return None
        # A's original atomic writer uses its normal agent file permissions, not
        # S's externally private protocol file mode. Keep no-link/single-link IO.
        st = self.client.no_links(path).stat()
        require(st.st_uid == os.getuid() and st.st_size <= 4 * 1024 * 1024, 'invalid local journal file')
        data = __import__('runtime.prepare', fromlist=['read_regular']).read_regular(path)
        value = self.client.decode(data)
        require(type(value) is dict and set(value) == {'declaration', 'seed', 'operations'} and
                type(value['operations']) is dict, 'damaged local intent journal')
        for key, envelope in value['operations'].items():
            require(key in ('accept', 'start') or key.startswith('cancel:'), 'unknown local operation')
            self.client.envelope(envelope, self.host)
        return value

    def save(self, pid, state):
        proto.atomic_write_json(str(self.state_path(pid)), state)

    def declaration(self, pid, enable):
        spec, binding, prompt = self.inputs(pid)
        for src in binding['sources']:
            repo = __import__('runtime.contract', fromlist=['repository']).repository(src['repository'])
            self.client.disjoint(self.protocol, repo)
        if enable is not None:
            require(type(enable) is dict and enable.get('by') == 'agentd-scheduler' and enable.get('ts'),
                    'only original scheduler release supported')
        seed = {'participant': pid, 'spec': spec, 'enable': enable, 'binding': binding,
                'promptDigest': prompt, 'runtimeRevision': self.REVISION}
        env = dict(binding['env'])
        env.update(AGENT_HOME=self.agent.adir(pid), AGENT_ROOT=str(self.workspace), AGENT_SELF=pid,
                   AGENTD_TASK='1', AGENTD_ROOT=str(self.workspace))
        env.update(self.agent._ask_inbox_env(spec))
        d = {'apiVersion': 'rund/v1alpha1', 'kind': 'Task',
             'metadata': {'id': runtime_id(self.host, pid), 'version': 1},
             'spec': {'node': self.host, 'sources': binding['sources'], 'prepare': [],
                      'toolchain': binding['toolchain'], 'argv': binding['argv'], 'cwd': '${work}',
                      'env': env, 'credentialRefs': [], 'attemptId': 'a1',
                      'requestId': 'release-' + fingerprint(seed)[:48]}}
        return self.client.validate(d), seed

    def exchange(self, pid, state, key, action, **fields):
        """Persist exact body before publish; an interrupted publish is not replayed.

        Only an existing exact request file can be re-read; absent file after a
        journaled intent is unknown, not a license to resend or allocate a rid.
        """
        self.verify_binding()
        ops = state['operations']
        e = dict(apiVersion=self.client.API, node=self.host, action=action, **fields)
        e['requestId'] = 'af-' + fingerprint([pid, key, e])[:48]
        prior = ops.get(key)
        if prior is None:
            ops[key] = e
            self.save(pid, state)
            self.client.send(self.protocol, e)
        else:
            require(prior == e, 'operation body changed')
            path = self.protocol / 'requests' / (e['requestId'] + '.json')
            require(path.exists(), 'journaled intent not published; unknown, no replay')
            require(self.client.decode(self.client.read(path)) == e, 'published body conflict')
        record = self.client.receipt(self.protocol, e['requestId'])
        if record is None:
            raise ValueError('awaiting original receipt; backend availability unknown: ' + e['requestId'])
        require(record.get('body') == self.client.request_body(e) and record.get('digest') ==
                self.client.digest(self.client.canonical(self.client.request_body(e))), 'not exact effect receipt')
        if record['status'] == 'pending':
            raise ValueError('accepted pending original receipt; no replay: ' + e['requestId'])
        result = record['result']
        require(type(result) is dict and result.get('status') not in ('blocked', 'rejected-or-blocked', 'pending'),
                'backend blocked; no new request id permitted')
        return result

    def validate_run(self, state, run):
        d = state['declaration']
        expected = {'apiVersion': 'rund-result/v1alpha1', 'kind': 'Task', 'node': self.host,
                    'id': d['metadata']['id'], 'attemptId': 'a1', 'generation': 1,
                    'declarationDigest': self.client.digest(self.client.canonical(d)),
                    'requestId': d['spec']['requestId'], 'declaration': d}
        require(type(run) is dict and all(run.get(k) == v for k, v in expected.items()), 'run binding mismatch')
        require(run.get('phase') in ('accepted', 'preparing', 'starting', 'running', 'canceling', 'cleanupUnknown', *FINAL),
                'unknown run phase')
        if run['phase'] in FINAL:
            require(run.get('endedAt'), 'terminal without endedAt')
            path = self.runtime_root / 'state/runs/Task' / run['id'] / 'a1/1/result.json'
            # Exact S publication is read only, never a Runtime convenience reader.
            result = self.client.decode(self.client.read(path, 4 * 1024 * 1024))
            require(result == run, 'terminal snapshot/receipt not immutable result')
            if run['phase'] == 'lost':
                require(run['exitCode'] is None and run['signal'] is None and not run['waitObserved'], 'lost with fake wait')
            if run['phase'] == 'succeeded':
                require(run['waitObserved'] is True and type(run['exitCode']) is int and run['exitCode'] == 0
                        and run['signal'] is None, 'success without real wait0')
        return run

    def snapshot_run(self, state):
        self.verify_binding()
        s = self.client.snapshot(self.protocol)
        require(s.get('apiVersion') == self.client.API and s.get('node') == self.host and
                s.get('semantics') == 'last-observed', 'invalid snapshot binding')
        require(not s.get('diagnostics') and ['Task', state['declaration']['metadata']['id']] not in s.get('blockedTargets', []),
                'snapshot blocked/diagnostic; retain last fact')
        runs = [r for r in s['runs'] if r.get('id') == state['declaration']['metadata']['id']]
        require(len(runs) <= 1, 'ambiguous snapshot target')
        return self.validate_run(state, runs[0]) if runs else None

    def project(self, pid, run):
        old = self.agent.read_pid(pid)
        if old and old.get('final'):
            return old
        # Accepted/preparing without recorded payload identity is not true running.
        # Original scheduler already reserves enabled/no-pid occupants.
        if not run.get('startedAt') and run['phase'] not in FINAL:
            return old
        fact = fingerprint(run)
        if old and old.get('runtime', {}).get('factDigest') == fact:
            return old  # do NOT renew a stale nonterminal observation each adapter tick
        observed = run.get('observation', {}).get('observedAt') or run.get('startedAt') or run['acceptedAt']
        if old and not terminal_phase(run) and observed < old.get('runtime', {}).get('factObservedAt', ''):
            return old  # a late older snapshot cannot roll a newer exact fact back
        phase = run['phase']
        terminal = phase in FINAL
        observed = run.get('observation', {}).get('observedAt') or run.get('startedAt') or run['acceptedAt']
        doc = {'gen': 0 if phase == 'canceled' and not run.get('startedAt') else 1,
               'pid': run.get('pid') or 0, 'procStart': run.get('procStart'), 'status': 'running',
               'final': terminal, 'restarts': 0, 'startedAt': agent_ts(run.get('startedAt') or run['acceptedAt']),
               'lastAliveAt': agent_ts(observed),
               'runtime': {'revision': self.REVISION, 'id': run['id'], 'attemptId': 'a1', 'generation': 1,
                           'requestId': run['requestId'], 'declarationDigest': run['declarationDigest'],
                           'phase': phase, 'reason': run['reason'], 'factDigest': fact,
                           'factObservedAt': observed, 'semantics': 'last-observed'}}
        if terminal:
            if phase == 'lost':
                doc.update(status='stale', exitcode=proto.EXITCODE_STALE)
            elif phase == 'canceled':
                doc.update(status='killed', exitcode=proto.EXITCODE_KILLED)
            elif run.get('signal'):
                doc.update(status='killed', exitcode=128 + run['signal'])
            else:
                doc.update(status='exited', exitcode=run['exitCode'] if run['exitCode'] is not None else 126)
            doc['endedAt'] = agent_ts(run['endedAt'])
        self.agent.write_pid(pid, doc)
        return doc

    def stop_valid(self, pid, req, gen, doc):
        expected = req.get('expectedGeneration')
        if expected is not None:
            if type(expected) is not int or expected != gen:
                self.agent.write_ack(pid, req['id'], 'noop', 'stale generation')
                return False
        else:
            ts = ts_epoch(req.get('ts'))
            if ts is None:
                self.agent.write_ack(pid, req['id'], 'rejected', 'missing/unparseable control timestamp')
                return False
            if doc and gen and ts < (ts_epoch(doc.get('startedAt')) or 0):
                self.agent.write_ack(pid, req['id'], 'noop', 'stale generation')
                return False
        if req.get('inject'):
            self.agent.write_ack(pid, req['id'], 'rejected', 'stop cannot inject')
            return False
        return True

    def service(self, pid):
        self.inputs(pid)
        doc = self.agent.read_pid(pid)
        controls = self.agent.pending_reqs(pid)
        for req in controls:
            if req.get('action') != 'stop':
                self.agent.write_ack(pid, req['id'], 'rejected', 'already final' if doc and doc.get('final') else
                                     'file one-shot supports stop only; no restart/clear/resident/auto')
        stops = [r for r in controls if r.get('action') == 'stop']
        if doc and doc.get('final'):
            # Reconcile only already-journaled controls. A later final stop is
            # noop without allocating another S request, even after cancellation.
            state = self.read_state(pid)
            known = {k[7:]: e for k, e in state['operations'].items() if k.startswith('cancel:')} if state else {}
            for req in stops:
                if req['id'] not in known:
                    self.agent.write_ack(pid, req['id'], 'noop', 'already final')
            for rid, e in known.items():
                value = self.exchange(pid, state, 'cancel:' + rid, 'cancel',
                                      **{k: e[k] for k in ('kind', 'id', 'attemptId', 'expectedGeneration')})
                terminal = self.validate_run(state, value.get('run', value))
                require(terminal['phase'] in FINAL, 'exact cancel done nonterminal; no ack/replay')
                self.agent.write_ack(pid, rid, 'applied' if terminal['phase'] == 'canceled' else 'noop',
                                     'runtime terminal: ' + terminal['reason'])
            return doc
        self.agent._seen_nonterminal.add(pid)
        enable_path = Path(proto.enable_path(str(self.workspace), pid))
        enable = proto.read_json(str(enable_path))
        require(not enable_path.exists() or enable is not None, 'unreadable scheduler enable')
        state = self.read_state(pid)
        if state is None:
            stops = [r for r in stops if self.stop_valid(pid, r, 0, doc)]
            if enable is None and not stops:
                return doc
            d, seed = self.declaration(pid, enable)
            state = {'declaration': d, 'seed': seed, 'operations': {}}
            self.save(pid, state)
        else:
            # Bind original permission forever, even when a queued cancel later sees enable.
            d, seed = self.declaration(pid, state['seed']['enable'])
            require(seed == state['seed'], 'immutable agent input changed')
            # Original ask route is fixed at admission; recipient liveness may later
            # change and notify_tick resolves it independently, not a new declaration.
            if state['seed']['enable'] is not None:
                require(enable == state['seed']['enable'], 'scheduler permission changed')
        d = state['declaration']
        fields = {'kind': 'Task', 'id': d['metadata']['id'], 'attemptId': 'a1'}
        accepted = self.exchange(pid, state, 'accept', 'accept', declaration=d,
                                 declarationDigest=self.client.digest(self.client.canonical(d)))
        if accepted is None:
            return doc
        self.validate_run(state, accepted)
        if 'start' in state['operations']:
            # A prior ambiguous start blocks ALL subsequent mutation (including new
            # cancel ids). Do not use a snapshot to fill its missing exact receipt.
            started = self.exchange(pid, state, 'start', 'start', **fields)
            self.validate_run(state, started)
        run = self.snapshot_run(state) or accepted
        doc = self.project(pid, run)
        gen = 1 if run.get('startedAt') else 0
        # Any previously sent cancel remains a barrier even if a request disappears.
        known = {k[7:]: e for k, e in state['operations'].items() if k.startswith('cancel:')}
        active = [r for r in stops if r['id'] in known or self.stop_valid(pid, r, gen, doc)]
        for req in active:
            known.setdefault(req['id'], None)
        for rid in known:
            value = self.exchange(pid, state, 'cancel:' + rid, 'cancel', **fields, expectedGeneration=1)
            if value is None:
                return doc
            terminal = value.get('run', value)
            self.validate_run(state, terminal)
            if terminal['phase'] not in FINAL:
                return doc  # exact done may be cleanupUnknown: no ack, no second cancel rid
            doc = self.project(pid, terminal)
            self.agent.write_ack(pid, rid, 'applied' if terminal['phase'] == 'canceled' else 'noop',
                                 'runtime terminal: ' + terminal['reason'])
        if known:
            return doc  # never start after any stop intent, even if enable arrives
        if ('start' in state['operations'] or run['phase'] == 'accepted') and enable is not None and state['seed']['enable'] is not None:
            started = self.exchange(pid, state, 'start', 'start', **fields)
            if started is not None:
                self.validate_run(state, started)
                doc = self.project(pid, started)
        # Read last observation, not a status/observe request per tick and not a new mutation.
        run = self.snapshot_run(state)
        if run:
            doc = self.project(pid, run)
            signature = fingerprint(run)
            previous, at = self.progress.get(pid, (None, time.monotonic()))
            if previous != signature:
                self.progress[pid] = (signature, time.monotonic())
            elif run['phase'] not in FINAL and time.monotonic() - at > 1:
                raise ValueError('no new backend run fact; owner availability unknown; retained last observation')
        return doc

    def tick(self):
        self.heartbeat()  # ONLY actual adapter-process heartbeat, not S owner availability
        results, diagnostics = {}, {}
        for pid in self.manifest['tasks']:
            try:
                results[pid] = self.service(pid)
            except (OSError, ValueError, KeyError, TypeError) as error:
                diagnostics[pid] = str(error)
                results[pid] = self.agent.read_pid(pid)
        proto.atomic_write_json(str(self.local / 'diagnostics.json'), {'at': proto.now_ts(), 'tasks': diagnostics})
        self.agent.notify_tick()
        return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('sandbox-root', 'workspace', 'runtime-root', 'runtime-source', 'protocol-root', 'manifest'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--seconds', type=float, default=0)
    args = vars(p.parse_args())
    seconds = args.pop('seconds')
    require(0 <= seconds <= 3600, 'bounded foreground duration required')
    with FileAdapter(**args) as adapter:
        end = time.monotonic() + seconds
        while True:
            print(json.dumps(adapter.tick(), sort_keys=True), flush=True)
            if time.monotonic() >= end:
                break
            time.sleep(.05)


if __name__ == '__main__':
    main()

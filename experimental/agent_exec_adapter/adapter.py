"""Agent semantics only. Fixed S owns prepare, execution, identity, locks/results.

Not imported by any active runner/loop. See README.md for the deliberately narrow
fixture contract and the cooperation/lease-FD requirement inherited from S.
"""
import argparse
from datetime import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time

A_ROOT = Path(__file__).resolve().parents[2]
if str(A_ROOT) not in sys.path:
    sys.path.insert(0, str(A_ROOT))
import envscrub
import proto
import registration
from runner import Runner, refresh_lock

S_REVISION = 'e4b79dce60f71b55899d3add5d2f127c90f1faaa'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def fingerprint(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def absolute(path):
    p = Path(path)
    require(p.is_absolute() and '..' not in p.parts, 'explicit absolute path required')
    for part in (p, *p.parents):
        require(not part.is_symlink(), 'symlink path refused: ' + str(part))
    return p


def inside(path, parent):
    return path != parent and parent in path.parents


def runtime_id(host, participant):
    return 'agent-' + fingerprint([host, participant])[:32]


def workdir(runtime_root, host, participant):
    return Path(runtime_root) / 'work/Task' / runtime_id(host, participant) / 'a1/1'


def load_runtime(source):
    source = absolute(source)
    dependency = json.loads((Path(__file__).with_name('runtime-dependency.json')).read_text())
    require(dependency['revision'] == S_REVISION, 'wrong fixed runtime revision')
    require(not list((source / 'runtime').rglob('*.pyc')), 'clean source-only S export required; cached bytecode refused')
    for rel, expected in dependency['files'].items():
        p = absolute(source / rel)
        require(p.is_file() and hashlib.sha256(p.read_bytes()).hexdigest() == expected,
                'fixed S file mismatch: ' + rel)
    # A private package name prevents an ambient `runtime` module from winning.
    package = '_p3a_s_' + S_REVISION
    if package in sys.modules:
        require(Path(sys.modules[package].__file__) == source / 'runtime/__init__.py',
                'runtime already imported from different source')
    else:
        spec = importlib.util.spec_from_file_location(package, source / 'runtime/__init__.py',
                    submodule_search_locations=[str(source / 'runtime')])
        module = importlib.util.module_from_spec(spec)
        sys.modules[package] = module
        spec.loader.exec_module(module)
    module = __import__(package + '.lifecycle', fromlist=['Runtime'])
    return module.Runtime, dependency, package


def agent_ts(value):
    if not value:
        return proto.now_ts()
    return datetime.fromisoformat(value).astimezone().strftime('%Y-%m-%d-%H-%M-%S.%f')[:-3]


class Adapter:
    """Single synchronous host writer. Progress only through explicit tick calls.

    Runtime's node flock fences the complete adapter lifetime. A workspace binding
    prevents another runtime-root/host/selection from bypassing that same fence.
    Runner is used ONLY for existing agent routing, control receipts and notices;
    none of its execution/observation/service methods is called.
    """
    def __init__(self, *, sandbox_root, workspace, runtime_root, runtime_source, manifest, hook=None):
        self.sandbox = absolute(sandbox_root)
        self.workspace = absolute(workspace)
        self.runtime_root = absolute(runtime_root)
        manifest = absolute(manifest)
        source = absolute(runtime_source)
        require(self.sandbox.is_dir(), 'sandbox root must already exist')
        require(all(inside(p, self.sandbox) for p in (self.workspace, self.runtime_root, manifest)),
                'workspace/runtime/manifest must be within explicit sandbox')
        require(not inside(self.runtime_root, self.workspace) and not inside(self.workspace, self.runtime_root),
                'runtime root and protocol workspace must be separate')
        require(all(p != q and not inside(p, q) and not inside(q, p)
                    for p, q in ((source, self.workspace), (source, self.runtime_root),
                                 (A_ROOT, self.workspace), (A_ROOT, self.runtime_root))),
                'source, runtime state and protocol workspace must be separate')
        require(not inside(manifest, self.workspace) and not inside(manifest, self.runtime_root),
                'fixture manifest must be outside protocol and runtime trees')
        require((self.workspace / 'agents').is_dir() and (self.workspace / 'env/host-id').is_file(),
                'explicit synthetic workspace and host-map required')
        self.manifest = json.loads(manifest.read_text())
        require(set(self.manifest) == {'experiment', 'host', 'tasks'}, 'unknown/missing manifest fields')
        require(self.manifest['experiment'] == 'p3a-one-shot-fixtures-v1', 'explicit experimental manifest required')
        self.host = self.manifest['host']
        require(isinstance(self.host, str) and self.host and self.host.replace('-', '').isalnum(), 'invalid host')
        require(type(self.manifest['tasks']) is dict and self.manifest['tasks'], 'explicit finite tasks required')
        for pid in self.manifest['tasks']:
            require(proto.parse_participant_id(pid)[0] == 'task', 'only Task family supported')
        Runtime, self.dependency, self.package = load_runtime(source)
        self.runtime = Runtime(str(self.runtime_root), self.host, hook=hook)
        try:
            binding = {'experiment': self.manifest, 'runtimeRoot': str(self.runtime_root),
                       'runtimeRevision': S_REVISION, 'runtimeSource': str(source)}
            marker = self.workspace / 'run/p3a-adapter-binding.json'
            proto.exclusive_write_json(str(marker), binding)
            require(proto.read_json(str(marker)) == binding, 'workspace bound to different adapter/root/selection')
            self.agent = Runner(str(self.workspace), self.host, '', 0.05)
            self.started_at = proto.now_ts()
            self.heartbeat()
            imports = {n: str(Path(m.__file__)) for n, m in sys.modules.items()
                       if n.startswith(self.package) and getattr(m, '__file__', None)}
            proto.atomic_write_json(str(self.runtime_root / 'state/adapter-imports.json'),
                                    {'revision': S_REVISION, 'imports': imports})
        except BaseException:
            self.runtime.kernel.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.runtime.kernel.close()

    def heartbeat(self):
        self.runtime.kernel.owned()
        refresh_lock(proto.agentd_lock_path(str(self.workspace), self.host), self.host, self.started_at)

    def inputs(self, pid):
        spec = self.agent.read_spec(pid)
        require(type(spec) is dict and not (set(spec) - registration.SPEC_KEYS), 'invalid spec')
        require(spec.get('host') == self.host, 'wrong host')
        require(spec.get('resident', False) is False and spec.get('restartPolicy') == 'one-shot',
                'only explicit nonresident one-shot supported; no auto/manual inference')
        require('profile' not in spec, 'model/persona commands unsupported in P3a')
        binding = self.manifest['tasks'][pid]
        require(type(binding) is dict and set(binding) == {'command', 'sources', 'argv', 'env', 'toolchain'},
                'invalid fixture binding')
        require(spec.get('command') == binding['command'] and
                spec['command'].startswith('p3a-fixture '), 'undeclared/unsupported command')
        require(spec.get('workdir') == str(workdir(self.runtime_root, self.host, pid)),
                'spec.workdir must explicitly name runtime independent work directory')
        for src in binding['sources']:
            # The experiment never prepares from a production source repository.
            repo = __import__(self.package + '.contract', fromlist=['repository']).repository(src['repository'])
            require(inside(absolute(repo), self.sandbox) and not inside(repo, self.workspace) and
                    not inside(repo, self.runtime_root), 'fixture Git source must be separate in sandbox')
        env = binding['env']
        require(type(env) is dict and envscrub.scrub_env(env, strip_third_party=True) == env,
                'fixture env may not declare inherited identity/secret-family keys')
        require(not (set(env) & {'HOME', 'TMPDIR', 'PATH', 'LC_ALL', 'RUND_LEASE_FD', 'AGENTD_ROOT'}),
                'runtime/adapter-owned environment key')
        prompt = absolute(Path(self.agent.adir(pid)) / 'prompt.md').read_bytes()
        require(prompt.strip(), 'registered prompt required')
        return spec, binding, hashlib.sha256(prompt).hexdigest()

    def declaration(self, pid, enable):
        spec, binding, prompt_digest = self.inputs(pid)
        if enable is not None:
            require(type(enable) is dict and enable.get('by') == 'agentd-scheduler' and enable.get('ts'),
                    'only original scheduler release supported')
        seed = {'participant': pid, 'spec': spec, 'enable': enable, 'binding': binding,
                'promptDigest': prompt_digest, 'runtimeRevision': S_REVISION}
        env = dict(binding['env'])  # Never inherit adapter process environment.
        env.update(AGENT_HOME=self.agent.adir(pid), AGENT_ROOT=str(self.workspace),
                   AGENT_SELF=pid, AGENTD_TASK='1', AGENTD_ROOT=str(self.workspace))
        env.update(self.agent._ask_inbox_env(spec))  # existing reaper/ask policy, not a copy
        d = {'apiVersion': 'rund/v1alpha1', 'kind': 'Task',
             'metadata': {'id': runtime_id(self.host, pid), 'version': 1},
             'spec': {'node': self.host, 'sources': binding['sources'], 'prepare': [],
                      'toolchain': binding['toolchain'], 'argv': binding['argv'], 'cwd': '${work}',
                      'env': env, 'credentialRefs': [], 'attemptId': 'a1',
                      'requestId': 'release-' + fingerprint(seed)[:48]}}
        return self.runtime.declaration(d), seed

    def admit(self, pid, enable):
        d, seed = self.declaration(pid, enable)
        marker = self.runtime_root / 'state/adapter' / (runtime_id(self.host, pid) + '.json')
        record = {'seed': seed, 'declaration': d}
        proto.exclusive_write_json(str(marker), record)
        require(proto.read_json(str(marker)) == record, 'immutable spec/prompt/enable/binding or route changed')
        run = self.runtime.accept_task(d, start=False)
        return run

    def project(self, pid, run):
        old = self.agent.read_pid(pid)
        if old and old.get('final') is True:
            return old
        phase = run['phase']
        terminal = phase in ('succeeded', 'failed', 'canceled', 'lost')
        started = run.get('startedAt')
        gen = 0 if phase == 'canceled' and not started else 1
        doc = {'gen': gen, 'pid': run.get('pid') or 0, 'procStart': run.get('procStart'),
               'status': 'running', 'final': terminal, 'restarts': 0,
               'startedAt': agent_ts(started or run['acceptedAt']), 'lastAliveAt': proto.now_ts(),
               'runtime': {'revision': S_REVISION, 'id': run['id'], 'attemptId': run['attemptId'],
                           'generation': run['generation'], 'requestId': run['requestId'],
                           'declarationDigest': run['declarationDigest'], 'phase': phase, 'reason': run['reason']}}
        if terminal:
            if phase == 'lost':
                doc.update(status='stale', exitcode=proto.EXITCODE_STALE)
            elif phase == 'canceled':
                doc.update(status='killed', exitcode=proto.EXITCODE_KILLED)
            elif run.get('signal'):
                doc.update(status='killed', exitcode=128 + run['signal'])
            else:
                # Prepare/start failure has no wait: explicit execution error, never exit0.
                doc.update(status='exited', exitcode=run['exitCode'] if run['exitCode'] is not None else 126)
            doc['endedAt'] = agent_ts(run['endedAt'])
        self.agent.write_pid(pid, doc)
        return doc

    def service(self, pid):
        require(pid in self.manifest['tasks'], 'participant not explicitly selected')
        self.inputs(pid)  # validate even queued/terminal declarations; never guess
        doc = self.agent.read_pid(pid)
        controls = self.agent.pending_reqs(pid)
        for req in controls:
            if req.get('action') != 'stop':
                self.agent.write_ack(pid, req['id'], 'rejected', 'already final' if doc and doc.get('final') else
                                     'P3a supports stop/cancel only; no restart/clear/inject')
        stops = [r for r in controls if r.get('action') == 'stop']
        if doc and doc.get('final'):
            for req in stops:
                self.agent.write_ack(pid, req['id'], 'noop', 'already final')
            return doc
        self.agent._seen_nonterminal.add(pid)
        enable_path = Path(proto.enable_path(str(self.workspace), pid))
        enable = proto.read_json(str(enable_path))
        require(not enable_path.exists() or enable is not None, 'unreadable scheduler release')
        if doc is None:
            current = []
            for req in stops:
                expected = req.get('expectedGeneration')
                if expected is not None and (type(expected) is not int or expected != 0):
                    self.agent.write_ack(pid, req['id'], 'noop', 'stale generation')
                else:
                    current.append(req)
            stops = current
        if enable is None and not stops:
            return None
        run = self.admit(pid, enable)
        for req in stops:
            expected = req.get('expectedGeneration')
            agent_gen = 1 if run.get('startedAt') else 0
            if expected is not None and (type(expected) is not int or expected != agent_gen):
                self.agent.write_ack(pid, req['id'], 'noop', 'stale generation')
                continue
            require(not req.get('inject'), 'stop cannot inject')
            ctl = {'apiVersion': 'rund-control/v1alpha1', 'kind': 'Task', 'id': run['id'],
                   'attemptId': 'a1', 'expectedGeneration': 1, 'action': 'cancel',
                   'requestId': 'cancel-' + fingerprint([pid, req])[:48]}
            receipt = self.runtime.control(ctl)
            run = self.runtime.observe_task(run['id'], 'a1')
            doc = self.project(pid, run)
            if doc['final']:
                self.agent.write_ack(pid, req['id'], 'applied' if run['phase'] == 'canceled' else 'noop',
                                     'runtime terminal: ' + run['reason'])
            # Ambiguous/pending receipt or cleanupUnknown is NOT acknowledged as stopped.
            if receipt.get('status') == 'pending' or not doc['final']:
                return doc
        if run['phase'] == 'accepted' and enable is not None:
            run = self.runtime.start_task(run['id'], 'a1')
        run = self.runtime.observe_task(run['id'], 'a1')
        return self.project(pid, run)

    def tick(self):
        self.heartbeat()  # actual live owner, never harness-written heartbeat
        results = {pid: self.service(pid) for pid in self.manifest['tasks']}
        self.agent.notify_tick()  # only original envelope/report policy; no supervision
        return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('sandbox-root', 'workspace', 'runtime-root', 'runtime-source', 'manifest'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--seconds', type=float, default=0, help='bounded foreground experiment; default one tick')
    a = p.parse_args()
    require(0 <= a.seconds <= 3600, 'bounded foreground duration required')
    with Adapter(**{k: getattr(a, k) for k in ('sandbox_root', 'workspace', 'runtime_root', 'runtime_source', 'manifest')}) as adapter:
        deadline = time.monotonic() + a.seconds
        while True:
            print(json.dumps(adapter.tick(), sort_keys=True), flush=True)
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)


if __name__ == '__main__':
    main()

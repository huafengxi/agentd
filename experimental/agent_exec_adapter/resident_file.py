"""Unwired local resident/auto/control policy client (task/j0se00).

B1 local intent gates A only; S acceptance gates independent owner ticks.
Exact held/terminal facts precede history and acknowledgment. No execution owner
or payload handles here. See RESIDENT_FILE.md for supported synthetic scope.
"""
import copy
import json
import time
from pathlib import Path

from .file_backend import FileAdapter, load_client as load_fixed_client
from .adapter import require, runtime_id, agent_ts, fingerprint
from .p3b import lifecycle_mapping, initial_workdir
import proto
from task_state import is_resident
from runner import ts_epoch

S_REVISION = '16d58bce251a7270191ae84b6c70d0a558784bd6'


def load_client(source):
    return load_fixed_client(source, revision=S_REVISION,
                             dependency='resident-file-dependency.json')


TERMINAL = ('succeeded', 'failed', 'canceled', 'lost', 'exited', 'stopped')


class ResidentFileAdapter(FileAdapter):
    REVISION = S_REVISION
    load_client = staticmethod(load_client)
    EXPERIMENT = 'agent-file-resident-fixtures-v1'

    def lifecycle(self, spec):
        return lifecycle_mapping(spec)['kind']

    def initial_workdir(self, pid, spec):
        return initial_workdir(self.runtime_root, self.host, pid, spec)

    def declaration(self, pid, enable):
        d, seed = super().declaration(pid, enable)
        spec = seed['spec']
        if is_resident(spec):
            d['spec']['env'].pop('AGENTD_TASK', None)
            for key in self.agent._ask_inbox_env(spec):
                d['spec']['env'].pop(key, None)
            d['spec']['env']['AGENTD_RESIDENT'] = '1'
        if self.lifecycle(spec) == 'Service':
            d['kind'] = 'Service'
            d['spec'].pop('attemptId')
            d['spec'].pop('requestId')
            d['spec'].update(desired='online', restart={
                'policy': lifecycle_mapping(spec)['policy'], 'initialSeconds': .1, 'maxSeconds': .4})
        return self.client.validate(d), seed

    def read_state(self, pid):
        path = self.state_path(pid)
        if not path.exists():
            return None
        st = self.client.no_links(path).stat()
        require(st.st_uid == __import__('os').getuid() and st.st_size <= 4 * 1024 * 1024,
                'invalid local journal')
        read = __import__('runtime.prepare', fromlist=['read_regular']).read_regular
        state = self.client.decode(read(path))
        required={'declaration', 'seed', 'operations', 'agentGeneration', 'control', 'archived', 'queuedRequest'}
        require(type(state) is dict and required <= set(state) <= required | {'generationOffset','priorDeclarations'},
                'damaged resident journal')
        require(type(state.get('generationOffset',0)) is int and state.get('generationOffset',0)>=0,
                'invalid agent generation offset')
        require(type(state['operations']) is dict and type(state['archived']) is list,
                'damaged resident operation journal')
        prior=state.get('priorDeclarations',[])
        require(type(prior) is list and len(prior)<=64,'invalid prior declarations')
        for old in prior:
            self.client.validate(old)
            require(old['kind']==state['declaration']['kind'] and old['metadata']['id']==state['declaration']['metadata']['id'] and old['spec']['node']==self.host,'prior declaration target mismatch')
        for envelope in state['operations'].values():
            self.client.envelope(envelope, self.host)
        return state

    def fields(self, state, generation=None):
        d = state['declaration']
        f = dict(kind=d['kind'], id=d['metadata']['id'],
                 attemptId=d['spec'].get('attemptId', 'service'))
        if generation is not None:
            f['expectedGeneration'] = generation
        return f

    def read_fact(self, path):
        return self.client.decode(self.client.read(path, 4 * 1024 * 1024))

    def validate_run(self, state, run, *, declaration=None, generation=None):
        d = declaration or state['declaration']
        expected = dict(apiVersion='rund-result/v1alpha1', kind=d['kind'], node=self.host,
                        id=d['metadata']['id'], attemptId=d['spec'].get('attemptId', 'service'),
                        declaration=d, declarationDigest=self.client.digest(self.client.canonical(d)))
        if d['kind'] == 'Task':
            expected.update(generation=1, requestId=d['spec']['requestId'])
        if generation is not None:
            expected['generation'] = generation
        require(type(run) is dict and all(run.get(k) == v for k, v in expected.items()), 'run binding mismatch')
        require(type(run.get('generation')) is int and run['generation'] > 0 and
                run.get('phase') in (*TERMINAL, 'accepted', 'preparing', 'starting', 'running', 'canceling', 'cleanupUnknown'),
                'invalid run identity/phase')
        if run['phase'] in TERMINAL:
            path = self.runtime_root / 'state/runs' / run['kind'] / run['id'] / run['attemptId'] / str(run['generation']) / 'result.json'
            require(run.get('endedAt') and self.read_fact(path) == run, 'terminal not immutable result')
            if run['phase'] == 'succeeded':
                require(run['waitObserved'] is True and run['exitCode'] == 0 and run['signal'] is None,
                        'success without actual wait0')
            if run['phase'] == 'lost':
                require(not run['waitObserved'] and run['exitCode'] is None and run['signal'] is None, 'fake lost wait')
        return run

    def validate_head(self, state, head):
        d = state['declaration']
        require(type(head) is dict and head.get('id') == d['metadata']['id'] and head.get('declaration') == d and
                type(head.get('generation')) is int and head['generation'] >= 0 and
                type(head.get('hold')) is bool, 'Service head binding mismatch')
        return head

    def current(self, state):
        self.verify_binding()
        snap = self.client.snapshot(self.protocol)
        d = state['declaration']; id = d['metadata']['id']
        require(snap.get('apiVersion') == self.client.API and snap.get('node') == self.host and
                snap.get('semantics') == 'last-observed', 'invalid snapshot')
        require(not snap.get('diagnostics') and [d['kind'], id] not in snap.get('blockedTargets', []),
                'snapshot blocked/diagnostic; retain last observation')
        head = None
        if d['kind'] == 'Service':
            heads = [h for h in snap['services'] if h.get('id') == id]
            require(len(heads) == 1, 'missing/ambiguous Service head')
            head = self.validate_head(state, heads[0])
        runs = [r for r in snap['runs'] if r.get('kind') == d['kind'] and r.get('id') == id and
                r.get('attemptId') == d['spec'].get('attemptId', 'service')]
        require(len(runs) <= 1, 'ambiguous current run')
        if head and head['generation'] == 0:
            require(not runs and head['hold'] and head['phase'] == 'stopped', 'invalid offline gen0')
            return None, head
        require(len(runs) == 1, 'missing current run')
        run = self.validate_run(state, runs[0], generation=head['generation'] if head else 1)
        if head:
            require(run['requestId'] == head['requestId'], 'Service request/head mismatch')
        return run, head

    def terminal_doc(self, doc, run):
        doc = copy.deepcopy(doc)
        phase = run['phase']
        if phase in ('canceled', 'stopped'):
            doc.update(status='killed', exitcode=proto.EXITCODE_KILLED)
        elif phase == 'lost' or run.get('pid') and not run.get('waitObserved'):
            doc.update(status='stale', exitcode=proto.EXITCODE_STALE)
        elif run.get('signal'):
            doc.update(status='killed', exitcode=128 + run['signal'])
        else:
            doc.update(status='exited', exitcode=run['exitCode'] if run['exitCode'] is not None else 126)
        doc['endedAt'] = agent_ts(run['endedAt'])
        return doc

    def archive(self, pid, state, run, doc):
        key = [run['kind'], run['attemptId'], run['generation']]
        if key in state['archived']:
            return
        require(run['phase'] in TERMINAL, 'cannot archive unknown termination')
        # archive_gen deliberately keeps the original weak best-effort IO contract.
        archived = self.terminal_doc(doc, run)
        if not doc:
            archived.update(gen=run['generation'] + state.get('generationOffset',0), pid=run.get('pid') or 0, procStart=run.get('procStart'))
        archived.update(final=False, runtimeHistory={'observedByA': bool(doc), 'fact': run})
        self.agent.archive_gen(pid, archived)
        state['archived'].append(key)
        self.save(pid, state)

    def project(self, pid, run, state, head=None, *, final=False, resumed=None):
        old = self.agent.read_pid(pid)
        if old and old.get('final'):
            return old
        if run is None:
            require(final and head and head['generation'] == 0 and head['hold'], 'no fabricated running gen0')
            gen = state.get('generationOffset',0)
            observed = state['seed']['spec']['createdAt']
            doc = dict(gen=gen, pid=0, procStart=None, status='killed', exitcode=proto.EXITCODE_KILLED,
                       final=True, restarts=0, startedAt=observed, endedAt=proto.now_ts(), lastAliveAt=observed,
                       runtime={'revision': self.REVISION, 'kind': 'Service', 'id': head['id'], 'generation': 0,
                                'phase': head['phase'], 'hold': head['hold'], 'semantics': 'last-observed'})
        else:
            if not run.get('startedAt') and run['phase'] not in TERMINAL:
                return old
            gen = head['generation'] + state.get('generationOffset',0) if head else state['agentGeneration']
            if not run.get('startedAt') and run['phase'] == 'canceled' and gen <= 1:
                gen = 0
            observed = run.get('observation', {}).get('observedAt') or run.get('startedAt') or run['acceptedAt']
            fact = fingerprint([run, head, final, resumed])
            if old and (old.get('gen', 0) > gen or old.get('runtime', {}).get('factDigest') == fact):
                return old
            if old and old.get('gen') == gen and observed < old.get('runtime', {}).get('factObservedAt', ''):
                return old
            # S may advance while A is down. Read exact original intermediate results;
            # never copy an old exit onto the current head or invent missing history.
            if head:
                for g in range(old.get('runtime', {}).get('generation', 1) if old else 1, head['generation']):
                    path = self.runtime_root / 'state/runs/Service' / run['id'] / 'service' / str(g) / 'result.json'
                    prior = self.read_fact(path)
                    self.validate_run(state, prior, declaration=prior['declaration'], generation=g)
                    require(prior['id'] == run['id'] and prior['declaration'] in [state['declaration'],*state.get('priorDeclarations',[])], 'history binding changed')
                    self.archive(pid, state, prior, old if old and g + state.get('generationOffset',0) == old.get('gen') else {})
            terminal = run['phase'] in TERMINAL
            is_final = final or terminal and run['kind'] == 'Task'
            reservation = head and head['phase'] == 'backoff' and not final
            doc = dict(gen=gen, pid=run.get('pid') or 0, procStart=run.get('procStart'), status='running',
                       final=bool(is_final), restarts=max(0, gen - 1),
                       startedAt=agent_ts(run.get('startedAt') or run['acceptedAt']), lastAliveAt=agent_ts(observed),
                       runtime=dict(revision=self.REVISION, kind=run['kind'], id=run['id'], attemptId=run['attemptId'],
                                    generation=run['generation'], requestId=run['requestId'],
                                    declarationDigest=run['declarationDigest'], phase=run['phase'], reason=run['reason'],
                                    factDigest=fact, factObservedAt=observed, semantics='last-observed'))
            if head:
                doc['runtime'].update(servicePhase=head['phase'], hold=head['hold'], nextStartAt=head['nextStartAt'])
            if terminal and not reservation:
                doc = self.terminal_doc(doc, run)
            if reservation:
                doc['runtime'].update(waitObserved=run['waitObserved'], exitCode=run['exitCode'],
                                      signal=run['signal'], endedAt=run['endedAt'])
            if (resumed is None and head and gen > 1 and (not old or old.get('gen', 0) < gen)
                    and state['seed']['spec'].get('restartPolicy') == 'auto'
                    and (not state['control'] or state['control']['stage'] == 'done')):
                resumed = gen - 1
            if resumed is not None:
                doc['resumed'] = resumed
            elif old and old.get('gen') == gen and 'resumed' in old:
                doc['resumed'] = old['resumed']
        if run is not None and run.get('startedAt') and 'pi-rpc-wrap.py' in state['seed']['spec'].get('command',''):
            doc['sock'] = proto.task_sock_path(str(self.workspace),proto.parse_participant_id(pid)[1])
        self.agent.write_pid(pid, doc)
        return doc

    def valid_control(self, pid, req, doc):
        action = req.get('action'); gen = (doc or {}).get('gen', 0)
        expected = req.get('expectedGeneration')
        timestamp = ts_epoch(req.get('ts'))
        if expected is not None and (type(expected) is not int or expected != gen) or (
                expected is None and gen and timestamp is not None and timestamp < (ts_epoch(doc.get('startedAt')) or 0)):
            self.agent.write_ack(pid, req['id'], 'noop', 'stale generation')
            return False
        if expected is None and timestamp is None:
            self.agent.write_ack(pid, req['id'], 'rejected', 'unknown control timestamp/generation')
            return False
        if action not in ('stop', 'restart', 'clear') or req.get('inject') and action != 'restart':
            self.agent.write_ack(pid, req['id'], 'rejected', 'unsupported action/inject')
            return False
        if action == 'clear' and doc is None:
            self.agent.write_ack(pid, req['id'], 'noop', 'no session yet; nothing cleared')
            return False
        return True

    def admit(self, pid, enable, queued):
        state = self.read_state(pid)
        d, seed = self.declaration(pid, state['seed']['enable'] if state else enable)
        if state:
            require(seed == state['seed'] and (seed['enable'] is None or seed['enable'] == enable),
                    'immutable spec/prompt/binding/enable changed')
            return state
        if queued and d['kind'] == 'Service':
            d['spec']['desired'] = 'offline'
        state = dict(declaration=d, seed=seed, operations={}, agentGeneration=1, control=None, archived=[],
                     queuedRequest=queued)
        self.save(pid, state)
        return state

    def finish_control(self, pid, state, outcome, detail):
        state['control'].update(stage='done', outcome=outcome, detail=detail)
        self.save(pid, state)
        self.agent.write_ack(pid, state['control']['request']['id'], outcome, detail)

    def control(self, pid, state, req=None):
        c = state['control']
        if c is None or c['stage'] == 'done':
            require(req is not None, 'missing control')
            doc = self.agent.read_pid(pid)
            if not self.valid_control(pid, req, doc):
                state['queuedRequest'] = None
                self.save(pid, state)
                return
            run, head = self.current(state)
            if run and run.get('startedAt') and (doc is None or
                    doc.get('runtime', {}).get('generation') != run['generation'] or
                    doc.get('runtime', {}).get('attemptId') != run['attemptId']):
                self.agent.write_ack(pid, req['id'], 'noop', 'agent observation no longer addresses current S run')
                state['queuedRequest'] = None
                self.save(pid, state)
                return
            c = dict(request=req, stage='stop', run=run, head=head, agentDoc=doc)
            state['control'] = c
            state['queuedRequest'] = None
            self.save(pid, state)  # all later agent mutations fenced, before publish
        continuing = req is None
        req = c['request']; key = 'control:' + req['id']; action = req['action']
        if continuing and c['stage'] in ('stop', 'replace'):
            offline = c['stage'] == 'stop' and c['head'] and c['head']['generation'] == 0
            require(offline or key + ':' + c['stage'] in state['operations'],
                    'control interrupted before publication; no automatic continuation')
        if c['stage'] == 'stop':
            run, head = c['run'], c['head']
            if head and head['generation'] == 0:
                require(head['hold'] and head['phase'] == 'stopped', 'offline Service not held')
                # The exact offline submit receipt is admission proof; control0 is unsupported.
                stopped, stopped_head = None, head
            else:
                result = self.exchange(pid, state, key + ':stop', 'cancel' if head is None else 'stop',
                                       **self.fields(state, run['generation']))
                if result.get('noop') and result.get('reason') == 'stale-generation':
                    self.finish_control(pid, state, 'noop', 'S generation advanced; no effect')
                    return
                if head is None:
                    stopped = self.validate_run(state, result.get('run', result))
                    stopped_head = None
                else:
                    stopped_head = self.validate_head(state, result)
                    require(stopped_head['hold'] and stopped_head['generation'] == run['generation'], 'stop not exact held generation')
                    path = self.runtime_root / 'state/runs/Service' / run['id'] / 'service' / str(run['generation']) / 'result.json'
                    # cleanupUnknown is an operation done but NOT a successful stop.
                    require(stopped_head['phase'] == 'stopped', 'stop done without group/lease convergence; no ack')
                    stopped = self.validate_run(state, self.read_fact(path), generation=run['generation'])
                require(stopped['phase'] in TERMINAL, 'stop done nonterminal; no ack/replay')
            if action == 'stop':
                self.project(pid, stopped, state, stopped_head, final=True)
                self.finish_control(pid, state, 'noop' if run and run['phase'] in TERMINAL else 'applied', 'exact stopped/held facts')
                return
            c.update(stage='history', stopped=stopped, stoppedHead=stopped_head)
            self.save(pid, state)
            # This stage is deliberately non-replayable. A crash/IO failure leaves
            # an unresolved barrier, not a compensation/recovery worker.
            if stopped and c['agentDoc']:
                self.archive(pid, state, stopped, c['agentDoc'])
            if action == 'clear':
                try:
                    self.agent._backup_and_clear_session(pid)
                except OSError as exc:
                    self.agent.write_ack(pid, req['id'], 'rejected', 'session backup/truncate failed: ' + str(exc))
                    raise
            elif req.get('inject'):
                self.agent.inject_premessage(pid, req)
            c['stage'] = 'replace'
            if state['declaration']['kind'] == 'Task':
                d = copy.deepcopy(state['declaration'])
                d['spec']['attemptId'] = 'a' + str(int(d['spec']['attemptId'][1:]) + 1)
                d['spec']['requestId'] = 'rerun-' + fingerprint([pid, req])[:48]
                c['replacement'] = d
            elif c['stoppedHead']['generation'] == 0:
                d = copy.deepcopy(state['declaration'])
                d['metadata']['version'] += 1
                d['spec']['desired'] = 'online'
                c['replacement'] = d
            self.save(pid, state)
        require(c['stage'] != 'history', 'history side effects interrupted; manual diagnosis required')
        if c['stage'] == 'replace':
            old = state['declaration']
            if old['kind'] == 'Task':
                result = self.exchange(pid, state, key + ':replace', 'rerun', id=old['metadata']['id'],
                                       oldAttempt=old['spec']['attemptId'], declaration=c['replacement'],
                                       declarationDigest=self.client.digest(self.client.canonical(c['replacement'])))
                newrun = self.validate_run(state, result, declaration=c['replacement'])
                newhead = None
            elif c['stoppedHead']['generation'] == 0:
                result = self.exchange(pid, state, key + ':replace', 'submit', declaration=c['replacement'],
                                       declarationDigest=self.client.digest(self.client.canonical(c['replacement'])))
                require(result.get('declaration') == c['replacement'] and result.get('generation') == 1, 'replacement submit failed')
                newhead = result
                path = self.runtime_root / 'state/runs/Service' / result['id'] / 'service/1/run.json'
                newrun = self.validate_run(state, self.read_fact(path.with_name('result.json') if path.with_name('result.json').exists() else path),
                                           declaration=c['replacement'], generation=1)
            else:
                result = self.exchange(pid, state, key + ':replace', 'restart',
                                       **self.fields(state, c['stopped']['generation']))
                newhead = self.validate_head(state, result)
                require(newhead['generation'] == c['stopped']['generation'] + 1 and not newhead['hold'], 'restart did not replace exact old generation')
                path = self.runtime_root / 'state/runs/Service' / result['id'] / 'service' / str(result['generation']) / 'run.json'
                newrun = self.validate_run(state, self.read_fact(path.with_name('result.json') if path.with_name('result.json').exists() else path),
                                           generation=result['generation'])
            require(newrun['phase'] == 'running' and newrun.get('pid') and newrun.get('procStart'), 'replacement not actual running; no applied ack')
            expected_rid = old['spec']['requestId'] if old['kind'] == 'Task' else self.client.inner_key(state['operations'][key + ':replace']['requestId'])
            if old['kind'] == 'Task':
                expected_rid = c['replacement']['spec']['requestId']
            require(newrun['requestId'] == expected_rid, 'replacement request/run mismatch')
            if 'replacement' in c:
                state['declaration'] = c['replacement']
            state['agentGeneration'] = (c['agentDoc'] or {}).get('gen', 0) + 1
            self.save(pid, state)
            self.project(pid, newrun, state, newhead, resumed=(c['agentDoc'] or {}).get('gen') if action == 'restart' else None)
            self.finish_control(pid, state, 'applied', 'actual replacement running, exact request/gen/identity')

    def service(self, pid):
        self.inputs(pid)
        doc = self.agent.read_pid(pid)
        state = self.read_state(pid)
        reqs = self.agent.pending_reqs(pid)
        if state and state['queuedRequest']:
            queued = state['queuedRequest']
            reqs = [queued] + [r for r in reqs if r['id'] != queued['id']]
        if state and state['control']:
            c = state['control']
            if c['stage'] == 'done':
                self.agent.write_ack(pid, c['request']['id'], c['outcome'], c['detail'])
                reqs = [r for r in reqs if r['id'] != c['request']['id']]
            else:
                self.control(pid, state)
                return self.agent.read_pid(pid)  # persisted barrier precedes ALL enable/agent controls
        if doc and doc.get('final'):
            for req in reqs:
                self.agent.write_ack(pid, req['id'], 'noop' if req.get('action') == 'stop' else 'rejected', 'already final')
            return doc
        self.agent._seen_nonterminal.add(pid)
        enable_path = Path(proto.enable_path(str(self.workspace), pid))
        enable = proto.read_json(str(enable_path))
        require(not enable_path.exists() or enable is not None, 'unreadable scheduler enable')
        if state is None:
            reqs = [r for r in reqs if self.valid_control(pid, r, doc)]
            if enable is None and not reqs:
                return doc
        state = self.admit(pid, enable, reqs[0] if reqs and state is None else None)
        d = state['declaration']
        action = 'accept' if d['kind'] == 'Task' else 'submit'
        admitted = self.exchange(pid, state, 'admit', action, declaration=d,
                                 declarationDigest=self.client.digest(self.client.canonical(d))) if not state['control'] else None
        if admitted is not None:
            if d['kind'] == 'Task':
                self.validate_run(state, admitted)
            else:
                self.validate_head(state, admitted)
        start_key = 'start:' + d['spec'].get('attemptId', 'service')
        if start_key in state['operations']:
            started = self.exchange(pid, state, start_key, 'start', **self.fields(state))
            self.validate_run(state, started)
        run, head = self.current(state)
        # Preserve Runner's natural Task-final-before-controls ordering.
        if run and run['kind'] == 'Task' and run['phase'] in TERMINAL:
            doc = self.project(pid, run, state)
            for req in reqs:
                self.agent.write_ack(pid, req['id'], 'noop' if req.get('action') == 'stop' else 'rejected', 'already final')
            return doc
        if run and run.get('startedAt'):
            doc = self.project(pid, run, state, head)
        if reqs:
            self.control(pid, state, reqs[0])
            return self.agent.read_pid(pid)
        if run and run['kind'] == 'Task' and run['phase'] == 'accepted' and enable is not None and state['seed']['enable'] is not None:
            started = self.exchange(pid, state, start_key, 'start', **self.fields(state))
            self.validate_run(state, started)
        run, head = self.current(state)
        if run:
            signature = fingerprint([run, head])
            previous, at = self.progress.get(pid, (None, time.monotonic()))
            if previous != signature:
                self.progress[pid] = (signature, time.monotonic())
            elif run['phase'] not in TERMINAL and time.monotonic() - at > 1:
                raise ValueError('no new backend fact; owner availability unknown')
            return self.project(pid, run, state, head)
        return doc


def main():
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('sandbox-root', 'workspace', 'runtime-root', 'runtime-source', 'protocol-root', 'manifest'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--seconds', type=float, default=0)
    args = vars(p.parse_args()); seconds = args.pop('seconds')
    require(0 <= seconds <= 3600, 'bounded duration required')
    with ResidentFileAdapter(**args) as a:
        end = time.monotonic() + seconds
        while True:
            print(json.dumps(a.tick(), sort_keys=True), flush=True)
            if time.monotonic() >= end:
                break
            time.sleep(.05)


if __name__ == '__main__':
    main()

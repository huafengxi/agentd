"""Opt-in P3b agent policy adapter. No process supervision lives here.

Only fixed S public Runtime methods execute/observe/stop/replace a payload.
Agent control journals deliberately block ambiguous partial effects; they are not
an automatic recovery/transaction platform. See P3B.md for the complete mapping.
"""
import copy
import json
from pathlib import Path

from .adapter import (Adapter as P3aAdapter, S_REVISION, agent_ts, fingerprint,
                      require, runtime_id, workdir)
import proto
from task_state import is_resident
from runner import ts_epoch


def lifecycle_mapping(spec):
    """The exact Runner.service finalize predicate, not a new success policy."""
    policy = spec.get('restartPolicy')
    require(policy in (None, 'one-shot', 'manual', 'auto'), 'unsupported restartPolicy')
    require('resident' not in spec or type(spec['resident']) is bool, 'invalid resident')
    final_on_exit = policy == 'one-shot' or (policy != 'auto' and not is_resident(spec))
    return {'kind': 'Task' if final_on_exit else 'Service',
            'finalOnExit': final_on_exit,
            'policy': 'always' if policy == 'auto' else 'manual'}


def initial_workdir(root, host, pid, spec):
    if lifecycle_mapping(spec)['kind'] == 'Task':
        return workdir(root, host, pid)
    return Path(root) / 'work/Service' / runtime_id(host, pid) / 'service/1'


class Adapter(P3aAdapter):
    EXPERIMENT = 'p3b-lifecycle-fixtures-v1'

    def lifecycle(self, spec):
        return lifecycle_mapping(spec)['kind']

    def initial_workdir(self, pid, spec):
        return initial_workdir(self.runtime_root, self.host, pid, spec)

    def declaration(self, pid, enable):
        d, seed = super().declaration(pid, enable)
        spec = seed['spec']
        mapping = lifecycle_mapping(spec)
        if is_resident(spec):
            d['spec']['env'].pop('AGENTD_TASK', None)
            for key in self.agent._ask_inbox_env(spec):
                d['spec']['env'].pop(key, None)
            d['spec']['env']['AGENTD_RESIDENT'] = '1'
        if mapping['kind'] == 'Service':
            d['kind'] = 'Service'
            d['spec'].pop('attemptId')
            d['spec'].pop('requestId')
            d['spec'].update(desired='online', restart={
                'policy': mapping['policy'], 'initialSeconds': 0.1, 'maxSeconds': 0.4})
        return self.runtime.declaration(d), seed

    def policy_path(self, pid):
        return self.runtime_root / 'state/adapter' / (runtime_id(self.host, pid) + '-p3b.json')

    def save_policy(self, pid, state):
        proto.atomic_write_json(str(self.policy_path(pid)), state)

    def admit(self, pid, enable, *, queued_stop=False, queued_control=False):
        # Once admitted, scheduler may move on; never change the original release.
        path = self.policy_path(pid)
        state = proto.read_json(str(path))
        d, seed = self.declaration(pid, state['seed']['enable'] if state else enable)
        if state:
            require(state['seed']['enable'] is None or enable == state['seed']['enable'],
                    'immutable scheduler release changed')
            require(seed == state['seed'], 'immutable spec/prompt/binding/route changed')
            return state
        if queued_control and d['kind'] == 'Service':
            d['spec']['desired'] = 'offline'
        state = {'seed': seed, 'declaration': d, 'agentGeneration': 0 if queued_stop else 1,
                 'attempt': 'a1', 'admission': 'pending', 'control': None}
        # No automatic replay of an interrupted Service submit / explicit rerun.
        proto.exclusive_write_json(str(path), state)
        if d['kind'] == 'Task':
            self.runtime.accept_task(d, start=False)
        else:
            receipt = self.runtime.submit_service(d, 'release-' + fingerprint(seed)[:48])
            state['submitReceipt'] = receipt
            self.save_policy(pid, state)
            if receipt.get('status') == 'pending' or 'generation' not in receipt:
                return state
        state['admission'] = 'done'
        self.save_policy(pid, state)
        return state

    def current(self, state, *, observe=False):
        d = state['declaration']; id = d['metadata']['id']
        if d['kind'] == 'Task':
            return (self.runtime.observe_task if observe else self.runtime.task)(id, state['attempt']), None
        service = self.runtime.tick_service(id) if observe else self.runtime.service(id)
        if service['generation']:
            run = self.runtime.read_run('Service', id, 'service', service['generation'])
        else:
            # Offline pre-start stop has no process/run/lease, only the real S head.
            run = None
        return run, service

    def project(self, pid, run, *, state, service=None, force_final=False, resumed=None):
        old = self.agent.read_pid(pid)
        if old and old.get('final') is True:
            return old
        if service is not None:
            gen = service['generation']
        else:
            gen = state['agentGeneration']
        phase = run['phase'] if run else service['phase']
        terminal = phase in ('succeeded', 'failed', 'canceled', 'lost', 'exited', 'stopped')
        final = force_final or (terminal and state['declaration']['kind'] == 'Task')
        if run is not None and not run.get('startedAt') and phase == 'canceled' and gen <= 1:
            gen = 0
        doc = {'gen': gen, 'pid': (run or {}).get('pid') or 0,
               'procStart': (run or {}).get('procStart'), 'status': 'running', 'final': final,
               'restarts': max(0, gen - 1),
               'startedAt': agent_ts((run or {}).get('startedAt') or (run or {}).get('acceptedAt')),
               'lastAliveAt': proto.now_ts(),
               'runtime': {'revision': S_REVISION, 'kind': state['declaration']['kind'],
                           'id': state['declaration']['metadata']['id'],
                           'attemptId': (run or {}).get('attemptId'),
                           'generation': (run or {}).get('generation', 0), 'phase': phase,
                           'reason': (run or {}).get('reason', 'offline-before-spawn'),
                           'requestId': (run or {}).get('requestId'),
                           'declarationDigest': (run or {}).get('declarationDigest')}}
        if service is not None:
            doc['runtime'].update(servicePhase=service['phase'], hold=service['hold'],
                                  nextStartAt=service['nextStartAt'])
        if resumed is not None:
            doc['resumed'] = resumed
        elif old and old.get('gen') == gen and 'resumed' in old:
            doc['resumed'] = old['resumed']
        backoff = service is not None and service['phase'] == 'backoff' and not force_final
        if backoff:
            # A nonresident auto still owns its scheduler slot/resources while S
            # waits to replace it. Do not expose a false release between runs.
            doc['runtime'].update(exitCode=run['exitCode'], signal=run.get('signal'),
                                  waitObserved=run['waitObserved'], endedAt=run['endedAt'])
        if (terminal and not backoff) or force_final:
            if run is None or phase in ('canceled', 'stopped') or force_final and not terminal:
                doc.update(status='killed', exitcode=proto.EXITCODE_KILLED)
            elif phase == 'lost' or run.get('endedAt') and not run.get('waitObserved') and run.get('pid'):
                doc.update(status='stale', exitcode=proto.EXITCODE_STALE)
            elif run.get('signal'):
                doc.update(status='killed', exitcode=128 + run['signal'])
            else:
                doc.update(status='exited', exitcode=run['exitCode'] if run['exitCode'] is not None else 126)
            doc['endedAt'] = agent_ts((run or {}).get('endedAt'))
        if old and old.get('gen', 0) < gen and state.get('archivedGeneration') != old.get('gen'):
            archived = copy.deepcopy(old)
            if old.get('runtime', {}).get('servicePhase') == 'backoff':
                fact = old['runtime']
                archived.update(status='stale' if not fact['waitObserved'] else ('killed' if fact['signal'] else 'exited'),
                                exitcode=proto.EXITCODE_STALE if not fact['waitObserved'] else
                                         (128 + fact['signal'] if fact['signal'] else fact['exitCode']),
                                endedAt=agent_ts(fact['endedAt']))
            self.agent.archive_gen(pid, archived)
            state['archivedGeneration'] = old.get('gen')
            self.save_policy(pid, state)
        self.agent.write_pid(pid, doc)
        return doc

    def stop_runtime(self, pid, state, run, service, req):
        if service is not None and not service['generation']:
            return service  # real offline head, no identity exists to signal
        ctl = {'apiVersion': 'rund-control/v1alpha1', 'kind': state['declaration']['kind'],
               'id': state['declaration']['metadata']['id'],
               'attemptId': run['attemptId'], 'expectedGeneration': run['generation'],
               'action': 'cancel' if service is None else 'stop',
               'requestId': 'stop-' + fingerprint([pid, req])[:48]}
        return self.runtime.control(ctl)

    def control(self, pid, req, state, run, service):
        doc = self.agent.read_pid(pid)
        action = req.get('action')
        if doc and doc.get('final'):
            self.agent.write_ack(pid, req['id'], 'noop' if action == 'stop' else 'rejected', 'already final')
            return doc
        expected = req.get('expectedGeneration')
        gen = doc.get('gen', 0) if doc else 0
        # Old CLI requests lack generation: their original timestamp must not
        # authorize stopping a successor that started after the request.
        if doc and gen and expected is None and ts_epoch(req.get('ts')) is None:
            self.agent.write_ack(pid, req['id'], 'rejected', 'control timestamp/generation unknown')
            return doc
        old_time = (doc and gen and expected is None and
                    ts_epoch(req.get('ts')) is not None and
                    ts_epoch(req['ts']) < (ts_epoch(doc.get('startedAt')) or 0))
        if (expected is not None and (type(expected) is not int or expected != gen)) or old_time:
            self.agent.write_ack(pid, req['id'], 'noop', 'stale generation')
            return doc
        if action not in ('stop', 'restart', 'clear') or (req.get('inject') and action != 'restart'):
            self.agent.write_ack(pid, req['id'], 'rejected', 'unsupported action/inject')
            return doc
        if action == 'clear' and doc is None:
            self.agent.write_ack(pid, req['id'], 'noop', 'no session yet; nothing cleared')
            return None
        state['control'] = {'id': req['id'], 'request': req, 'agentGeneration': gen,
                            'status': 'pending', 'ownerEpoch': self.runtime.kernel.epoch}
        self.save_policy(pid, state)  # ambiguous effects never replayed, even after owner re-open
        already_terminal = run is not None and run['phase'] in ('succeeded', 'failed', 'canceled', 'lost', 'exited', 'stopped')
        receipt = self.stop_runtime(pid, state, run, service, req)
        if receipt.get('status') == 'pending':
            return doc
        run, service = self.current(state)
        if run is not None and run['phase'] not in ('succeeded', 'failed', 'canceled', 'lost', 'exited', 'stopped'):
            return self.project(pid, run, state=state, service=service)
        if action == 'stop':
            doc = self.project(pid, run, state=state, service=service, force_final=True)
            outcome = 'noop' if already_terminal else 'applied'
            detail = 'runtime confirmed terminal/group and lease collected; lifecycle finalized'
        else:
            # Archive after S stop confirmation, before session backup/truncation,
            # exactly the old agent-layer order. Do NOT finalize the intermediate
            # Task cancellation: external restart/clear is still a nonfinal life.
            stopped = self.stopped_snapshot(pid, run, service, state)
            if doc is not None:
                self.agent.archive_gen(pid, stopped)
            state['archivedGeneration'] = gen
            self.save_policy(pid, state)
            note = ''
            if action == 'clear':
                try:
                    _backup, note = self.agent._backup_and_clear_session(pid)
                except OSError as exc:
                    # Same conservative boundary as legacy: no new generation.
                    # Retain pending journal after partial backup/truncation.
                    self.agent.write_ack(pid, req['id'], 'rejected', 'session backup/truncate failed: ' + str(exc))
                    return doc
            elif req.get('inject'):
                self.agent.inject_premessage(pid, req)
            if service is not None and not service['generation']:
                d = copy.deepcopy(service['declaration'])
                d['metadata']['version'] += 1
                d['spec']['desired'] = 'online'
                receipt = self.runtime.submit_service(d, 'start-' + fingerprint([pid, req])[:48])
                if receipt.get('status') == 'pending' or 'generation' not in receipt:
                    return doc
            elif service is not None:
                ctl = {'apiVersion': 'rund-control/v1alpha1', 'kind': 'Service',
                       'id': run['id'], 'attemptId': 'service', 'expectedGeneration': run['generation'],
                       'action': 'restart', 'requestId': 'restart-' + fingerprint([pid, req])[:48]}
                receipt = self.runtime.control(ctl)
                if receipt.get('status') == 'pending' or receipt.get('phase') == 'cleanupUnknown':
                    return doc
            else:
                d = copy.deepcopy(state['declaration'])
                d['spec']['attemptId'] = 'a' + str(int(state['attempt'][1:]) + 1)
                d['spec']['requestId'] = 'rerun-' + fingerprint([pid, req])[:48]
                self.runtime.rerun_task(run['id'], state['attempt'], d)
                state['attempt'] = d['spec']['attemptId']
                state['agentGeneration'] = gen + 1
            run, service = self.current(state)
            # Replacement allocation/start failure is not a successful restart.
            if not run or not run.get('pid') or run['phase'] != 'running':
                return doc
            doc = self.project(pid, run, state=state, service=service,
                               resumed=gen if action == 'restart' and gen else None)
            outcome, detail = 'applied', note + 'new gen %d%s' % (doc['gen'], ' (blank session)' if action == 'clear' else '')
        state['control'].update(status='done', outcome=outcome, detail=detail)
        self.save_policy(pid, state)
        self.agent.write_ack(pid, req['id'], outcome, detail)
        return doc

    def stopped_snapshot(self, pid, run, service, state):
        # A history view, not another pid writer or runtime result writer.
        doc = copy.deepcopy(self.agent.read_pid(pid) or {})
        fact = run or {}
        if fact.get('phase') in ('canceled', 'stopped'):
            status, code = 'killed', proto.EXITCODE_KILLED
        elif fact.get('pid') and not fact.get('waitObserved'):
            status, code = 'stale', proto.EXITCODE_STALE
        elif fact.get('signal'):
            status, code = 'killed', 128 + fact['signal']
        else:
            status, code = 'exited', fact.get('exitCode') if fact.get('exitCode') is not None else 126
        doc.update(status=status, exitcode=code, final=False,
                   endedAt=agent_ts(fact.get('endedAt')))
        doc['runtimeStop'] = {'phase': (run or service)['phase'],
                              'waitObserved': (run or {}).get('waitObserved')}
        return doc

    def service(self, pid):
        require(pid in self.manifest['tasks'], 'participant not explicitly selected')
        self.inputs(pid)
        doc = self.agent.read_pid(pid)
        state = proto.read_json(str(self.policy_path(pid)))
        reqs = self.agent.pending_reqs(pid)
        completed = (state or {}).get('control')
        if completed and completed['status'] == 'done':
            self.agent.write_ack(pid, completed['id'], completed['outcome'], completed['detail'])
            reqs = [r for r in reqs if r['id'] != completed['id']]
        if doc and doc.get('final'):
            for req in reqs:
                self.agent.write_ack(pid, req['id'], 'noop' if req.get('action') == 'stop' else 'rejected', 'already final')
            return doc
        self.agent._seen_nonterminal.add(pid)
        enable_path = Path(proto.enable_path(str(self.workspace), pid))
        enable = proto.read_json(str(enable_path))
        require(not enable_path.exists() or enable is not None, 'unreadable scheduler release')
        if state is None and doc is None:
            valid = []
            for req in reqs:
                expected = req.get('expectedGeneration')
                if expected is not None and (type(expected) is not int or expected != 0):
                    self.agent.write_ack(pid, req['id'], 'noop', 'stale generation')
                elif req.get('action') not in ('stop', 'restart', 'clear') or (req.get('inject') and req.get('action') != 'restart'):
                    self.agent.write_ack(pid, req['id'], 'rejected', 'unsupported action/inject')
                elif req.get('action') == 'clear':
                    self.agent.write_ack(pid, req['id'], 'noop', 'no session yet; nothing cleared')
                else:
                    valid.append(req)
            reqs = valid
            if enable is None and not reqs:
                return None
        queued_stop = doc is None and bool(reqs) and reqs[0].get('action') == 'stop'
        queued_control = doc is None and state is None and bool(reqs)
        state = self.admit(pid, enable, queued_stop=queued_stop, queued_control=queued_control)
        if state['admission'] != 'done':
            return doc  # ambiguous submit; no replay
        control = state.get('control')
        if control and control['status'] == 'pending':
            return doc  # explicit manual boundary; no supervision/replay on partial control
        if control and control['status'] == 'done':
            self.agent.write_ack(pid, control['id'], control['outcome'], control['detail'])
        # Observe/finalize first like Runner.service, except an accepted Task or
        # pre-start offline Service must not acquire a spurious agent generation.
        run, service = self.current(state, observe=not queued_control and not
                                    (reqs and state['declaration']['kind'] == 'Service'))
        if run and (run.get('startedAt') or run['phase'] != 'accepted'):
            doc = self.project(pid, run, state=state, service=service)
        for req in reqs:
            doc = self.control(pid, req, state, run, service)
            if (state.get('control') or {}).get('status') == 'pending' or doc and doc.get('final'):
                return doc
            run, service = self.current(state)
        if service is None and run['phase'] == 'accepted' and enable is not None:
            run = self.runtime.start_task(run['id'], state['attempt'])
            state['agentGeneration'] = 1
            self.save_policy(pid, state)
        run, service = self.current(state, observe=True)
        return self.project(pid, run, state=state, service=service)


if __name__ == '__main__':
    from .adapter import main
    main(adapter_class=Adapter)

"""Agent publication adapter and read-only rund execution views."""
import base64
import hashlib
import json
from pathlib import Path
import sys
import proto

EXECUTION_KEYS = frozenset({'command', 'workdir', 'host', 'resources', 'needs', 'restartPolicy', 'resident'})

def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def client(root):
    config = json.loads((Path(root) / 'run/execution.json').read_text())
    sys.path.insert(0, config['runtimeSource'])
    from runtime.config import node_config
    return config, node_config(config['nodeConfig'])

def object_id(root, participant):
    reference = proto.read_json(str(Path(proto.agent_dir(root, participant)) / 'execution.json'))
    return reference['id'] if reference else None

def facts(root, participant):
    identifier = object_id(root, participant)
    if identifier is None: return None, None, None
    _, node = client(root)
    from runtime.protocol import get
    declaration, state = get(Path(node['sharedRoot']), identifier)
    task = state
    if declaration and declaration['kind'] == 'Service':
        task = get(Path(node['sharedRoot']), state['currentTask'])[1] if state and state.get('currentTask') else None
    return declaration, state, task

def state_view(root, participant):
    """Derived application view, never persisted; unknown remains unknown."""
    declaration, state, task = facts(root, participant)
    if not declaration: return None
    final = (declaration['kind'] == 'Task' and task and task['phase'] in ('succeeded', 'failed', 'canceled')) or (declaration['kind'] == 'Service' and state and state['phase'] == 'stopped')
    task = task or {}
    phase = task.get('phase', 'accepted')
    process = task.get('process') or {}
    code = task.get('exitCode')
    if task.get('signal') is not None: code = 128 + task['signal']
    if phase == 'canceled': code = proto.EXITCODE_CANCELED
    status = 'exited' if phase in ('succeeded', 'failed') else 'killed' if phase == 'canceled' else 'running' if phase in ('starting', 'running', 'canceling') else phase
    def timestamp(value):
        if not value: return None
        from datetime import datetime
        return datetime.fromisoformat(value).astimezone().strftime('%Y-%m-%d-%H-%M-%S.%f')
    execution = declaration.get('task', declaration)
    view = dict(status=status, final=bool(final), exitcode=code,
                pid=process.get('pid'), procStart=process.get('start'),
                startedAt=timestamp(task.get('startedAt')), endedAt=timestamp(task.get('finishedAt')),
                lastAliveAt=timestamp(task.get('updatedAt')), gen=task.get('id'),
                runtime={'id': declaration['id'], 'task': task.get('id'), 'phase': phase})
    argv = execution.get('argv', [])
    command = base64.b64decode(argv[-1]).decode() if any(p.endswith('/exec_task.py') for p in argv) else ' '.join(argv)
    if 'pi-rpc-wrap.py' in command:
        view['sock'] = proto.task_sock_path(root, participant.split('/', 1)[1])
    return view

def metadata(root, participant):
    value = proto.read_json(proto.spec_path(root, participant))
    if value is None: return None
    declaration, _, _ = facts(root, participant)
    if declaration:
        execution = declaration.get('task', declaration)
        argv = execution.get('argv', [])
        command = base64.b64decode(argv[-1]).decode() if any(p.endswith('/exec_task.py') for p in argv) else ' '.join(argv)
        value = dict(value, host=declaration['node'], workdir=execution.get('cwd', ''),
                     command=command, resources=execution.get('resources'),
                     needs=execution.get('needs', []), resident=declaration['kind'] == 'Service',
                     restartPolicy=declaration.get('restart', 'one-shot'))
    return value

def submit(root, participant, spec):
    config, node = client(root)
    existing_id = object_id(root, participant)
    from runtime.protocol import get
    if existing_id:
        declaration, _ = get(Path(node['sharedRoot']), existing_id)
        if declaration is None: raise ValueError('Referenced rund declaration has not arrived: ' + existing_id)
        return declaration
    if spec.get('provides'): raise ValueError('provides was removed; publish explicit output files')
    target = config['nodes'][spec['host']]
    root = str(root)
    from notifications import Notifications
    policy = Notifications(root, target['host'], target.get('aliases', ''), .5)
    ask = {key: value.replace(root, target['workspace']) for key, value in policy._ask_inbox_env(spec).items()}
    env = dict(target.get('env', {}), AGENTD_CODE=target['agentCode'], PI_HOST_DIR=target['piCode'],
               AGENT_HOME=str(Path(target['workspace']) / 'agents' / participant), AGENT_ROOT=target['workspace'],
               AGENT_SELF=participant, AGENTD_ROOT=target['workspace'], PYTHONDONTWRITEBYTECODE='1')
    resident = spec.get('resident') is True
    env.update({'AGENTD_RESIDENT': '1'} if resident else dict(ask, AGENTD_TASK='1'))
    cwd = spec.get('workdir') or target['workspace']
    if cwd == '~' or cwd.startswith('~/'): cwd = target['home'] + cwd[1:]
    needs = list(target.get('needs', [])) + list(spec.get('needs', []))
    if any(not p.startswith(('/', '${')) for p in needs): raise ValueError('needs must reference target files')
    task = dict(argv=[target['python'], target['agentCode'] + '/exec_task.py', base64.b64encode(spec['command'].encode()).decode()],
                env=env, cwd=cwd, needs=needs)
    if not resident: task['resources'] = spec.get('resources', ['serial'])
    service = resident or spec.get('restartPolicy') == 'auto'
    declaration = dict(kind='Service' if service else 'Task', id='a-' + fingerprint([target['host'], participant])[:40],
                       publisher='agent-' + node['node'], node=target['host'], revision=1)
    if service: declaration.update(version=1, desired='online', restart='always' if spec.get('restartPolicy') == 'auto' else 'manual', task=task)
    else: declaration.update(task, desired='run')
    from runtime.config import publish
    # Publish the object first; retries use the same deterministic identity.
    publish(node['sharedRoot'], declaration, writer=node['node'], publishers=node['publishers'])
    proto.atomic_write_json(str(Path(proto.agent_dir(root, participant)) / 'execution.json'), {'id': declaration['id']})
    clean = {key: value for key, value in spec.items() if key not in EXECUTION_KEYS and key != 'provides'}
    proto.atomic_write_json(proto.spec_path(root, participant), clean)
    return declaration

def desired(root, participant, value):
    _, node = client(root)
    identifier = object_id(root, participant)
    if not identifier: raise ValueError('Participant has no execution object')
    from runtime.cli import set_desired
    declaration, _, _ = facts(root, participant)
    return set_desired(node, identifier, 'offline' if declaration['kind'] == 'Service' and value == 'cancel' else value)


def service_action(root, participant, action):
    """Route normal runctl lifecycle commands to the execution node."""
    import shlex
    import subprocess
    config, node = client(root)
    declaration, _, _ = facts(root, participant)
    if not declaration or declaration['kind'] != 'Service':
        raise ValueError('Only a Service can restart; create a new Task participant')
    target = config['nodes'][declaration['node']]
    argv = [target['home'] + '/.rund/bin/runctl', action, declaration['id']]
    if declaration['node'] != node['node']:
        argv = ['ssh', '-o', 'BatchMode=yes', target['host'], shlex.join(argv)]
    subprocess.run(argv, check=True, timeout=150)


def clear_service_session(root, participant):
    """Run a finite file operation through rund after the Service stops."""
    import time
    import uuid
    config, node = client(root)
    declaration, _, _ = facts(root, participant)
    if declaration['kind'] != 'Service': raise ValueError('Create a new Task participant for a fresh session')
    service_action(root, participant, 'stop')
    target = config['nodes'][declaration['node']]
    identifier = 'session-clear-' + uuid.uuid4().hex
    script = '''from pathlib import Path
import os, shutil, sys
session=Path(sys.argv[1]);backup=Path(sys.argv[2])
session.parent.mkdir(parents=True,exist_ok=True)
if session.exists() and session.stat().st_size:
    backup.parent.mkdir(parents=True,exist_ok=True)
    with session.open('rb') as src,backup.open('xb') as dst:shutil.copyfileobj(src,dst)
with session.open('w'):pass
os.chown(session,os.getuid(),os.getgid())
'''
    task = dict(kind='Task',id=identifier,publisher='agent-'+node['node'],node=declaration['node'],revision=1,desired='run',
                argv=[target['python'],'-c',script,str(Path(target['workspace'])/'agents'/participant/'session/session.jsonl'),
                      str(Path(target['workspace'])/'run/backup'/(identifier+'.jsonl'))],
                cwd=target['workspace'],env={},needs=[])
    from runtime.config import publish
    from runtime.protocol import get
    publish(node['sharedRoot'],task,writer=node['node'],publishers=node['publishers'])
    end=time.monotonic()+150
    while time.monotonic()<end:
        state=get(Path(node['sharedRoot']),identifier)[1]
        if state and state['phase'] in ('succeeded','failed','canceled','unknown'):
            if state['phase']!='succeeded' or not state.get('waitObserved'):raise ValueError('Session clear failed: '+str(state))
            service_action(root,participant,'start')
            return state
        time.sleep(.3)
    raise ValueError('Session clear Task pending: '+identifier)

"""Publish agent execution objects directly; message semantics stay in agentd."""
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
import proto
from task_state import is_resident


def fingerprint(v):
    return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def declaration(c, node, pid, spec, generation, ask_env):
    command=spec['command']
    for spelling in ('$AGENT_ROOT/agent-platform/agentd/','${AGENT_ROOT}/agent-platform/agentd/','$AGENT_ROOT/agentd/','${AGENT_ROOT}/agentd/'):
        command=command.replace(spelling,'$AGENTD_CODE/')
    code=c['agentCode'];root=c['workspace']
    env=dict(c.get('env',{}),AGENTD_CODE=code,PI_HOST_DIR=c['piCode'],AGENT_HOME=str(Path(root)/'agents'/pid),AGENT_ROOT=root,AGENT_SELF=pid,AGENTD_ROOT=root,PYTHONDONTWRITEBYTECODE='1')
    if is_resident(spec):env['AGENTD_RESIDENT']='1'
    else:env.update(ask_env);env['AGENTD_TASK']='1'
    cwd=spec.get('workdir') or root
    if cwd=='~' or cwd.startswith('~/'):cwd=c['home']+cwd[1:]
    needs=list(c.get('needs',[]))
    if spec.get('needs'):needs.append(str(Path(root)/'agents'/pid/'dependencies.ready'))
    task=dict(argv=[c['python'],code+'/exec_task.py',base64.b64encode(command.encode()).decode()],env=env,cwd=cwd,needs=needs)
    if not is_resident(spec):task['resources']=spec.get('resources',['serial'])
    service=spec.get('restartPolicy')=='auto' or is_resident(spec) and spec.get('restartPolicy')!='one-shot'
    d=dict(kind='Service' if service else 'Task',id='a-'+fingerprint([c['host'],pid,generation])[:40],publisher='agent-'+node['node'],node=c['host'],revision=1)
    if service:d.update(version=1,desired='online',restart='always' if spec.get('restartPolicy')=='auto' else 'manual',task=task)
    else:d.update(task,desired='run')
    return d


def submit(root,pid,spec):
    """CLI publication. The deployment supplies target configs without secrets."""
    config_path=Path(root)/'run/execution.json'
    if not config_path.exists():return None  # Protocol-only offline workspace.
    config=json.loads(config_path.read_text());c=config['nodes'][spec['host']]
    sys.path.insert(0,config['runtimeSource'])
    from runtime.config import node_config,publish,discover
    from runner import Runner
    local=node_config(config['nodeConfig'])
    policy=Runner(root,c['host'],c.get('aliases',''),.5)
    ask=policy._ask_inbox_env(spec)
    ask={k:v.replace(str(root),c['workspace']) for k,v in ask.items()}
    d=declaration(c,local,pid,spec,1,ask)
    # Publish an immutable intent before the object, allowing origin-only repair.
    reference=Path(proto.agent_dir(root,pid))/'execution.json'
    prior=proto.read_json(str(reference))
    if prior is not None:
        if prior['declaration']!=d:raise ValueError('execution intent changed')
    else:proto.atomic_write_json(str(reference),{'declaration':d,'inputDigest':fingerprint(spec)})
    found,errors=discover(local['sharedRoot'])
    if errors:raise ValueError('; '.join(errors))
    if d['id'] in found:
        existing=found[d['id']][1]
        fields=lambda x:{k:v for k,v in x.items() if k not in ('revision','desired','cancelMode')}
        if fields(existing)!=fields(d):raise ValueError('execution object identity collision')
        return existing
    publish(local['sharedRoot'],d,writer=local['node'],publishers=local['publishers'])
    return d

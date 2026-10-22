#!/usr/bin/env python3
"""Agent policy over serviced objects. Payload spawn/wait/signals stay in Owner."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
from datetime import datetime
sys.path.insert(0,str(Path(__file__).resolve().parent))
import proto
from runner import Runner,acquire_single_instance_lock,setup_logging,ts_epoch
from task_state import is_resident


def fingerprint(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def ats(s):return datetime.fromisoformat(s).astimezone().strftime('%Y-%m-%dT%H:%M:%S%z') if s else proto.now_ts()

class ObjectRunner(Runner):
    def __init__(self,c):
        self.config=c;self.host=c['host']
        sys.path.insert(0,c['runtimeSource'])
        from runtime import protocol
        from runtime.config import FINAL,node_config
        from runtime.store import write,read,require
        self.client=protocol;self.write=write;self.read=read;self.require=require;self.FINAL=FINAL
        self.node=node_config(c['nodeConfig']);self.shared=Path(self.node['sharedRoot'])
        lock,_=acquire_single_instance_lock(c['workspace'],c['host'])
        self.ownerlock=lock
        super().__init__(c['workspace'],c['host'],c.get('aliases',''),c.get('interval',.5),lockfile=proto.agentd_lock_path(c['workspace'],c['host']),lock_host=c['host'])
        self.journals=Path(self.root)/'run/object-agent';self.journals.mkdir(mode=0o700,parents=True,exist_ok=True)
    def journal(self,pid):return self.journals/(fingerprint(pid)+'.json')
    def put(self,d):return self.client.publish(self.shared,d,writer=self.node['node'],publishers=self.node['publishers'])
    def set_desired(self,id,value):return self.client.desired(self.shared,id,value,writer=self.node['node'],publishers=self.node['publishers'])
    def build(self,pid,spec,j):
        cmd=spec['command']
        for spelling in ('$AGENT_ROOT/agent-platform/agentd/','${AGENT_ROOT}/agent-platform/agentd/','$AGENT_ROOT/agentd/','${AGENT_ROOT}/agentd/'):
            cmd=cmd.replace(spelling,'$AGENTD_CODE/')
        code=self.config['agentCode'];env=dict(self.config.get('env',{}),AGENTD_CODE=code,PI_HOST_DIR=self.config['piCode'],AGENT_HOME=self.adir(pid),AGENT_ROOT=self.root,AGENT_SELF=pid,AGENTD_ROOT=self.root,PYTHONDONTWRITEBYTECODE='1')
        if is_resident(spec):env['AGENTD_RESIDENT']='1'
        else:env.update(self._ask_inbox_env(spec));env['AGENTD_TASK']='1'
        cwd=spec.get('workdir') or self.root
        if cwd=='~' or cwd.startswith('~/'):cwd=self.config['home']+cwd[1:]
        task=dict(argv=[self.config['python'],code+'/exec_task.py',base64.b64encode(cmd.encode()).decode()],env=env,cwd=str(Path(cwd).resolve()),needs=self.config.get('needs',[]))
        service=spec.get('restartPolicy')=='auto' or is_resident(spec) and spec.get('restartPolicy')!='one-shot'
        d=dict(kind='Service' if service else 'Task',id='a-'+fingerprint([self.host,pid,j['generation']])[:40],publisher='agent-'+self.node['node'],node=self.node['node'],revision=1)
        if service:d.update(version=1,desired='online',restart='always' if spec.get('restartPolicy')=='auto' else 'manual',task=task)
        else:d.update(task,desired='run')
        return d
    def fact(self,d):
        _,s=self.client.get(self.shared,d['id'])
        if d['kind']=='Service':
            if not s or not s.get('currentTask'):return s,None
            _,ts=self.client.get(self.shared,s['currentTask']);return s,ts
        return None,s
    def project(self,pid,j,d,s,t):
        if not t or not t.get('startedAt') and t['phase'] not in self.FINAL:return self.read_pid(pid)
        if t['id'] not in j['children']:
            old=self.read_pid(pid)
            if old and j['children']:self.archive_gen(pid,old)
            j['children'].append(t['id']);self.write(self.journal(pid),j)
        gen=j['generation']+len(j['children'])-1
        terminal=t['phase'] in self.FINAL;final=terminal and d['kind']=='Task'
        proc=t.get('process') or {}
        doc=dict(gen=gen,pid=proc.get('pid',0),procStart=int(proc['start']) if proc.get('start') else None,status='running',final=final,restarts=max(0,gen-1),startedAt=ats(t.get('startedAt') or t['updatedAt']),lastAliveAt=ats(t['updatedAt']),runtime=dict(id=d['id'],task=t['id'],phase=t['phase'],declarationDigest=t['declarationDigest'],semantics='last-observed'))
        if terminal:
            doc.update(status='killed' if t['phase']=='canceled' or t.get('signal') else 'exited',exitcode=proto.EXITCODE_KILLED if t['phase']=='canceled' else 128+t['signal'] if t.get('signal') else t['exitCode'] if t['exitCode'] is not None else 126,endedAt=ats(t['finishedAt']))
            if d['kind']=='Service' and s['phase']=='backoff':doc.update(status='running',final=False)
        if 'pi-rpc-wrap.py' in self.read_spec(pid).get('command',''):doc['sock']=proto.task_sock_path(self.root,proto.parse_participant_id(pid)[1])
        old=self.read_pid(pid)
        if old!=doc:self.write_pid(pid,doc)
        return doc
    def service(self,pid):
        spec=self.read_spec(pid)
        if not spec or not proto.host_matches(spec,self.local_ids):return
        doc=self.read_pid(pid);reqs=self.pending_reqs(pid)
        if doc and doc.get('final'):
            for r in reqs:self.write_ack(pid,r['id'],'noop' if r.get('action')=='stop' else 'rejected','already final')
            return
        self._seen_nonterminal.add(pid)
        jp=self.journal(pid);j=self.read(jp) if jp.exists() else None
        enable=proto.read_json(proto.enable_path(self.root,pid))
        if j is None:
            if not enable and not reqs:return
            self.require(doc is None or doc.get('status') in proto.CRASH_STATUSES,'legacy live participant needs explicit handoff')
            j=dict(generation=(doc or {}).get('gen',0)+1,children=[],control=None,inputDigest=fingerprint(spec))
            d=self.build(pid,spec,j)
            if reqs:d['desired']='offline' if d['kind']=='Service' else 'cancel'
            j['declaration']=d;self.write(jp,j);self.put(d)
        d=j['declaration'];self.require(j['inputDigest']==fingerprint(spec),'registered execution changed')
        self.put(d);s,t=self.fact(d);doc=self.project(pid,j,d,s,t) or doc
        control=j.get('control')
        if not control and reqs:
            r=reqs[0];expected=r.get('expectedGeneration');gen=(doc or {}).get('gen',0)
            if expected is not None and expected!=gen or expected is None and ts_epoch(r.get('ts')) is None:
                self.write_ack(pid,r['id'],'noop','stale or unknown generation');return
            if expected is None and doc and (ts_epoch(r.get('ts')) or 0)<(ts_epoch(doc.get('startedAt')) or 0):
                self.write_ack(pid,r['id'],'noop','stale control timestamp');return
            if r.get('action') not in ('stop','restart','clear'):
                self.write_ack(pid,r['id'],'rejected','unsupported action');return
            if doc and doc.get('runtime',{}).get('task') and t and doc['runtime']['task']!=t['id']:
                self.write_ack(pid,r['id'],'noop','Task advanced');return
            control=dict(request=r,stage='stop',oldTask=t['id'] if t else None,oldDoc=doc);j['control']=control;self.write(jp,j)
        if not control:return
        r=control['request'];action=r['action']
        if control['stage']=='stop':
            value='offline' if d['kind']=='Service' else 'cancel';d=self.set_desired(d['id'],value);j['declaration']=d;self.write(jp,j)
            s,t=self.fact(d)
            if d['kind']=='Service' and (not s or s['phase']!='stopped'):return
            if t and t['phase'] not in self.FINAL:return
            if action=='stop':
                doc=self.project(pid,j,d,s,t) or dict(gen=j['generation'],pid=0,procStart=None,status='killed',exitcode=proto.EXITCODE_KILLED,startedAt=proto.now_ts())
                doc.update(final=True,endedAt=proto.now_ts());self.write_pid(pid,doc);self.write_ack(pid,r['id'],'applied','Task closed');j['control']=None;self.write(jp,j);return
            if doc:self.archive_gen(pid,doc)
            control['stage']='history';self.write(jp,j)
            if action=='clear':self._backup_and_clear_session(pid)
            elif r.get('inject'):self.inject_premessage(pid,r)
            control['stage']='replace';self.write(jp,j)
        self.require(control['stage']!='history','session mutation interrupted; inspect before continuing')
        if control['stage']=='replace':
            if d['kind']=='Task':
                j['generation']=(control['oldDoc'] or {}).get('gen',0)+1;j['children']=[];d=self.build(pid,spec,j);j['declaration']=d;control['stage']='wait';self.write(jp,j);self.put(d)
            else:d=self.set_desired(d['id'],'online');j['declaration']=d;control['stage']='wait';self.write(jp,j)
        d=j['declaration'];s,t=self.fact(d)
        if t and t['id']!=control['oldTask'] and t['phase']=='running':
            self.project(pid,j,d,s,t);self.write_ack(pid,r['id'],'applied','replacement Task running');j['control']=None;self.write(jp,j)


def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--log-file');a=p.parse_args();setup_logging(a.log_file,'INFO')
    c=json.loads(Path(a.config).read_text());r=ObjectRunner(c)
    try:r.run()
    finally:r.ownerlock.close()

if __name__=='__main__':main()

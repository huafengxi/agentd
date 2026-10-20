#!/usr/bin/env python3
"""Agent protocol policy using the local S owner; no payload Popen/wait/signals.

The resident adapter supplies exact control barriers, generation/history mapping,
notifications and unknown handling. This entry replaces its finite fixture inputs
with current host-routed protocol registrations and caller-pinned Git sources.
"""
import argparse
import base64
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import proto
import registration
from runner import Runner,acquire_single_instance_lock,refresh_lock,setup_logging
from task_state import is_resident
from experimental.agent_exec_adapter.resident_file import ResidentFileAdapter
from experimental.agent_exec_adapter.file_backend import load_client
from experimental.agent_exec_adapter.adapter import absolute,require,runtime_id
from experimental.agent_exec_adapter.p3b import lifecycle_mapping

log=logging.getLogger('agentd.rund')


class RundAgent(ResidentFileAdapter):
    def __init__(self,config):
        if config.get('legacyCutoverFile'):
            require(not config.get('legacyCutover'),'choose one legacy evidence source')
            proofs=json.loads(Path(config['legacyCutoverFile']).read_text())
            require(type(proofs) is dict,'legacy cutover evidence object required')
            config=dict(config,legacyCutover=proofs)
        self.config=config
        require(config.get('apiVersion')=='rund-agent/v1','unsupported agent runtime config')
        self.workspace=absolute(Path(config['workspace']).resolve())
        self.runtime_root=absolute(Path(config['runtimeRoot']).resolve())
        self.protocol=absolute(Path(config['protocolRoot']).resolve())
        self.host=config['host'];self.REVISION=config['runtimeRevision']
        self.client,self.dependency=load_client(absolute(Path(config['runtimeSource']).resolve()),revision=self.REVISION,dependency=config['runtimeManifest'])
        self.client.disjoint(self.protocol,self.runtime_root)
        self.local=self.workspace/'run/rund-agent';self.local.mkdir(parents=True,exist_ok=True)
        # Same stable local inode as the former runner; never a synced lock.
        self.lock,heartbeat=acquire_single_instance_lock(str(self.workspace),self.host)
        try:
            bind=dict(host=self.host,runtimeRoot=str(self.runtime_root),protocolRoot=str(self.protocol))
            marker=self.local/'binding.json';proto.exclusive_write_json(str(marker),bind)
            require(proto.read_json(str(marker))==bind,'agent workspace runtime binding conflict')
            self.agent=Runner(str(self.workspace),self.host,config.get('aliases',''),config.get('interval',.5))
            self.started_at=proto.now_ts();self.progress={};self.package='runtime'
            self.manifest={'tasks':{}}
            self.verify_binding();self.heartbeat()
        except BaseException:
            self.lock.close();raise

    def inputs(self,pid):
        spec=self.agent.read_spec(pid)
        require(type(spec) is dict and not (set(spec)-registration.SPEC_KEYS),'invalid registered spec')
        require(proto.host_matches(spec,self.agent.local_ids),'wrong registered host')
        lifecycle_mapping(spec)
        prompt=Path(self.agent.adir(pid))/'prompt.md'
        prompt_digest=hashlib.sha256(prompt.read_bytes() if prompt.exists() else b'').hexdigest()
        command=spec['command']
        # Current callers use AGENTD_CODE. Preserve queued commands written by
        # the old caller while binding their implementation to fixed Git code.
        for spelling in ('$AGENT_ROOT/agent-platform/agentd/','${AGENT_ROOT}/agent-platform/agentd/','$AGENT_ROOT/agentd/','${AGENT_ROOT}/agentd/'):
            command=command.replace(spelling,'$AGENTD_CODE/')
        compiled=base64.b64encode(command.encode()).decode()
        sources=self.config['sources']
        require(any(s['alias']=='agentd' for s in sources),'pinned agentd source required')
        env=dict(self.config.get('env',{}),AGENTD_CODE='${code.agentd}',PYTHONDONTWRITEBYTECODE='1')
        if any(s['alias']=='pi' for s in sources):env['PI_HOST_DIR']='${code.pi}'
        binding=dict(command=spec['command'],sources=sources,argv=[self.config['python'],'${code.agentd}/exec_task.py',compiled],env=env,toolchain={'python':self.config['python']})
        return spec,binding,prompt_digest

    def declaration(self,pid,enable):
        d,seed=super().declaration(pid,enable)
        spec=seed['spec'];workdir=spec.get('workdir')
        home=self.config['home']
        if workdir and workdir.startswith('~/'):workdir=home+workdir[1:]
        d['spec']['context']=dict(cwd=str(Path(workdir or str(self.workspace)).resolve()),home=home,path=self.config['path'],inheritEnv=self.config.get('inheritEnv',[]))
        return self.client.validate(d),seed

    def service(self,pid):
        doc=self.agent.read_pid(pid)
        if doc and not doc.get('final') and self.read_state(pid) is None:
            # No fake S execution record for a process launched by the former
            # runner. One-time cutover must identify it and confirm its absence.
            proof=self.config.get('legacyCutover',{}).get(pid)
            require(type(proof) is dict and all(proof.get(k)==doc.get(k) for k in ('gen','pid','procStart')),
                    'legacy execution requires explicit cutover evidence')
            require(proof.get('groupEmpty') is True,'legacy process group closure not evidenced')
            require(not self.agent.probe_alive(pid,doc.get('pid'),doc.get('procStart')),
                    'legacy execution still alive; no duplicate launch')
            if doc.get('status')=='running':self.agent.observe(pid,doc)
            if lifecycle_mapping(self.agent.read_spec(pid))['kind']=='Task':
                # An already-ended ordinary legacy task is finalized by its
                # existing policy, never rerun as a new runtime attempt.
                observed=self.agent.read_pid(pid)
                require(observed.get('status') in proto.CRASH_STATUSES,'legacy Task has no final observation')
                observed['final']=True
                self.agent.write_pid(pid,observed)
        return super().service(pid)

    def admit(self,pid,enable,queued):
        prior=self.read_state(pid)
        state=super().admit(pid,enable,queued)
        if prior is None:
            old=self.agent.read_pid(pid)
            proof=self.config.get('legacyCutover',{}).get(pid)
            if old and proof:
                require(old.get('gen')==proof['gen'] and old.get('status')!='running','legacy history not converged')
                self.agent.archive_gen(pid,old)
                state['generationOffset']=old['gen']
                state['agentGeneration']=old['gen']+1
                self.save(pid,state)
        return state

    def tick(self):
        self.manifest['tasks']={pid:{} for pid in proto.list_participants(str(self.workspace))
                                if (spec:=self.agent.read_spec(pid)) and proto.host_matches(spec,self.agent.local_ids)}
        return super().tick()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',required=True);p.add_argument('--log-file');p.add_argument('--runtime-source')
    a=p.parse_args();setup_logging(a.log_file,'INFO')
    config=json.loads(Path(a.config).read_text())
    if a.runtime_source:config['runtimeSource']=a.runtime_source
    with RundAgent(config) as agent:
        while True:
            result=agent.tick()
            diagnostics=proto.read_json(str(agent.local/'diagnostics.json')) or {}
            for pid,error in diagnostics.get('tasks',{}).items():log.warning('%s: %s',pid,error)
            time.sleep(config.get('interval',.5))


if __name__=='__main__':main()

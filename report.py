#!/usr/bin/env python3
"""Read agent metadata and rund execution facts; no scheduling inference."""
import argparse
import json
import os
from pathlib import Path
import sys
import time
import os, sys
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import proto
from execution import client, facts
from session_tools import current_tools, ts_to_epoch
from platform_paths import workspace_root

SECTION_TITLES = {'abnormal':'异常与阻塞','active':'未结束任务','recent':'最近终态', 'terminal':'终态任务','system':'基础设施健康','stats':'状态统计','bots':'常驻服务','topics':'主题'}

def escape(value): return str(value if value is not None else '-').replace('|', '\\|').replace('\n', ' ')

def build_report(root, now, show_terminal=False, all_terminal=False):
    config, node = client(root)
    from runtime.cli import status
    health = status(node)
    rows = []
    abnormal = []
    for participant in proto.list_participants(root):
        spec = proto.read_json(proto.spec_path(root, participant))
        if not spec: continue
        try:
            declaration, state, task = facts(root, participant)
        except (OSError, ValueError, KeyError) as exc:
            abnormal.append(participant + ': ' + str(exc)); continue
        if not declaration: continue
        task = task or {}
        phase = task.get('phase', 'awaiting-state')
        execution = declaration.get('task', declaration)
        row = {'id':participant,'kind':declaration['kind'],'host':declaration['node'], 'name':spec.get('name', ''),
               'phase':phase,'desired':declaration['desired'],'task':task.get('id'),
               'exitCode':task.get('exitCode'),'signal':task.get('signal'),'finishedAt':task.get('finishedAt'),
               'needs':execution.get('needs', []),'resources':execution.get('resources'), 'reason':task.get('reason'), 'current_tools': current_tools(proto.agent_dir(root, participant), proto.execution_state(root, participant), now) if phase == 'running' else '-'}
        rows.append(row)
        if phase in ('unknown', 'failed', 'preparing') or row['reason']:
            abnormal.append(participant + ': ' + phase + ' ' + str(row['reason'] or ''))
    def table(values):
        out=['| 参与方 | 名称 | 节点 | 状态 | desired | Task | 退出码 / 信号 | 当前工具 |','|---|---|---|---|---|---|---|---|']
        for row in values:
            out.append('| '+' | '.join(escape(row[key]) for key in ('id','name','host','phase','desired','task'))+' | '+escape(row['exitCode'])+' / '+escape(row['signal'])+' | '+escape(row['current_tools'])+' |')
        return '\n'.join(out) if values else '（无）'
    terminal = sorted((r for r in rows if r['phase'] in ('succeeded','failed','canceled')), key=lambda r:r['finishedAt'] or '', reverse=True)
    active=[r for r in rows if r['phase'] not in ('succeeded','failed','canceled')]
    system='```json\n'+json.dumps({'node':node['node'],'total':health['total'],'diagnostics':health.get('diagnostics',[]),'blockedTargets':health.get('blockedTargets',[]),'admission':node.get('admission')},indent=2,ensure_ascii=False)+'\n```'
    sections={'abnormal':'\n'.join(abnormal) or '（无异常）','active':table(active),'recent':table(terminal[:10]),
              'terminal':table(terminal if all_terminal else terminal[:20]),'system':system,
              'stats': '\n'.join(phase+': '+str(sum(r['phase']==phase for r in rows)) for phase in sorted(set(r['phase'] for r in rows))),
              'bots':table([r for r in rows if r['kind']=='Service']),
              'topics':'\n'.join(p.name for p in sorted((Path(root)/'agents/topic').glob('*')) if p.is_dir()) or '（无）'}
    text='# Agent 执行状态\n\n'+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(now))+'\n'
    for key,title in SECTION_TITLES.items():
        if key=='terminal' and not show_terminal: continue
        text+='\n## '+title+'\n\n'+sections[key]+'\n'
    return text,rows

def extract_section(text,key):
    marker='## '+SECTION_TITLES[key]+'\n'
    return text.split(marker,1)[1].split('\n## ',1)[0].strip()+'\n' if marker in text else ''

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default=os.environ.get('AGENTD_ROOT') or workspace_root(__file__))
    parser.add_argument('--out')
    parser.add_argument('--finalized',action='store_true')
    parser.add_argument('--all',action='store_true')
    parser.add_argument('-s','--section',choices=SECTION_TITLES)
    args=parser.parse_args()
    text,_=build_report(args.root,time.time(),args.finalized or args.all or args.section=='terminal',args.all)
    if args.section:text=extract_section(text,args.section)
    if args.out:proto.atomic_write(args.out,text)
    else:sys.stdout.write(text)

if __name__=='__main__':main()

"""Read-only file dependencies from serviced declarations and target state."""
import argparse
import json
import os, sys
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import proto
from execution import facts
from platform_paths import workspace_root

def collect_unsat(root):
    rows = []
    for participant in proto.list_participants(root):
        declaration, state, task = facts(root, participant)
        if declaration and task and task['phase'] == 'preparing':
            rows.append({'id': participant, 'needs': declaration.get('task', declaration).get('needs', []),
                         'reason': task.get('reason'), 'phase': task['phase']})
    return rows

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default=workspace_root(__file__))
    args = parser.parse_args()
    print(json.dumps({'ok': True, 'waiting': collect_unsat(args.root)}, ensure_ascii=False))

if __name__ == '__main__': main()

#!/usr/bin/env python3
"""Read-only per-commit/per-path provenance and preservation verification.

python recovery/verify.py --source /path/to/platform-source.git
Optional --origin /path/to/agentd.git checks actual pushed refs/objects too.
No fetch, network, checkout, index write, deletion, deployment or service calls.
"""
import argparse
import json
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / 'recovery'


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args])


def tree(repo, rev):
    result = {}
    for entry in git(repo, 'ls-tree', '-rz', rev).split(b'\0'):
        if entry:
            info, path = entry.split(b'\t', 1)
            mode, kind, oid = info.decode().split()
            result[path.decode()] = {'mode': mode, 'blob': oid}
    return result


def load(name):
    return json.loads((DATA / name).read_text())


def owned(path):
    if path.startswith('agentd/'):
        return path[7:]
    if path in ('platform_paths.py', 'test_platform_paths.py'):
        return path
    if path.startswith('portable/') and path.split('/')[1] in (
            'README.md', 'build.py', 'cli.py', 'files.json', 'run.sh', 'test_portable.py'):
        return path
    return None


def changes(repo, parent, commit):
    old, new = tree(repo, parent), tree(repo, commit)
    return {p for p in old.keys() | new.keys() if old.get(p) != new.get(p)}


def verify(source=None, origin=None):
    refs = load('source-refs.json')
    mapping = load('source-map.json')
    adaptation = load('adaptations.json')
    target = tree(REPO, 'HEAD')
    for repo in [REPO] + ([Path(origin)] if origin else []):
        git(repo, 'merge-base', '--is-ancestor', refs['original_main'], 'HEAD')
        for ref, oid in refs['preserved_target_refs'].items():
            actual = git(repo, 'rev-parse', ref).decode().strip() if (repo != REPO or ref == 'refs/heads/main') else git(REPO, 'rev-parse', 'refs/remotes/origin/' + ref.removeprefix('refs/heads/')).decode().strip()
            if ref == 'refs/heads/main':
                git(repo, 'merge-base', '--is-ancestor', oid, actual)
            else:
                assert actual == oid, (ref, actual, oid)
            objects = refs['objects'][ref]
            readable = subprocess.check_output(
                ['git', '-C', str(repo), 'cat-file', '--batch-check'],
                input=('\n'.join(objects) + '\n').encode()).decode().splitlines()
            assert len(readable) == len(objects) and all(' missing' not in x for x in readable)
            original_objects = {line.split()[0] for line in git(repo, 'rev-list', '--objects', oid).decode().splitlines()}
            assert original_objects == set(objects), ref
        git(repo, 'fsck', '--full', '--strict')
    expected_parent = refs['original_main']
    for row in mapping['records']:
        commit = row['Target-Commit']
        source_commit = row['Source-Commit']
        if source:
            assert git(source, 'rev-parse', source_commit + '^1').decode().strip() == row['Source-Parent']
            before = tree(source, row['Source-Parent'])
            after = tree(source, source_commit)
            source_changes = changes(source, row['Source-Parent'], source_commit)
            assert {p for p in source_changes if owned(p)} == {x['Source-Path'] for x in row['changes']}, source_commit
            for item in row['changes']:
                p = item['Source-Path']
                assert before.get(p) == ({'mode': item['old_mode'], 'blob': item['old_blob']} if item['old_blob'] else None)
                assert after.get(p) == ({'mode': item['mode'], 'blob': item['blob']} if item['blob'] else None)
            for item in row['excluded']:
                def normalized(value):
                    return {'mode': value[0], 'blob': value[2]} if isinstance(value, list) else value
                assert before.get(item['Source-Path']) == normalized(item['old'])
                assert after.get(item['Source-Path']) == normalized(item['new'])
            excluded = {x['Source-Path'] for x in row['excluded']}
            assert excluded == {p for p in source_changes if p == 'install.py' or (p.startswith('portable/') and not owned(p))}
        if not commit:
            assert not row['changes'], row
            continue
        assert git(REPO, 'rev-parse', commit + '^1').decode().strip() == expected_parent
        before = tree(REPO, expected_parent)
        after = tree(REPO, commit)
        assert changes(REPO, expected_parent, commit) == {x['Target-Path'] for x in row['changes']}
        for item in row['changes']:
            p = item['Target-Path']
            assert item['deleted'] == (item['blob'] is None)
            assert before.get(p) == ({'mode': item['old_mode'], 'blob': item['old_blob']} if item['old_blob'] else None), p
            assert after.get(p) == ({'mode': item['mode'], 'blob': item['blob']} if item['blob'] else None), p
        message = git(REPO, 'show', '-s', '--format=%B', commit).decode()
        assert 'Source-Commit: ' + source_commit in message and 'rd6-a-171006' in message
        expected_parent = commit
    assert expected_parent == adaptation['projection_commit']
    assert git(REPO, 'rev-parse', adaptation['adaptation_commit'] + '^1').decode().strip() == expected_parent
    assert changes(REPO, expected_parent, adaptation['adaptation_commit']) == {x['Target-Path'] for x in adaptation['changes']}
    for item in adaptation['changes']:
        assert target.get(item['Target-Path']) == item['adapted'], item
    net = load('net-paths.json')
    assert {p for p in target if not p.startswith('recovery/')} == {x['Target-Path'] for x in net if not x['deleted']}
    for item in net:
        assert target.get(item['Target-Path']) == item['delivered'], item
        if not item['adaptation']:
            assert item['delivered'] == item['projected'] == item['source_frozen'], item
    for forbidden in ('install.py', 'portable/install.py', 'portable/example.json'):
        assert forbidden not in target
    if source:
        sequence = git(source, 'rev-list', '--first-parent', '--reverse', mapping['baseline'] + '..' + mapping['frozen']).decode().splitlines()
        assert sequence == [x['Source-Commit'] for x in mapping['records']]
        baseline = {p[7:]: v for p, v in tree(source, mapping['baseline']).items() if p.startswith('agentd/')}
        assert baseline == tree(REPO, refs['original_main'])
    if origin:
        assert git(origin, 'rev-parse', 'refs/heads/main') == git(REPO, 'rev-parse', 'HEAD')
    print(json.dumps({'PASS': True, 'source_records': len(mapping['records']),
                      'projected_commits': sum(bool(r['Target-Commit']) for r in mapping['records']),
                      'path_checks': len(net), 'adapted_paths': len(adaptation['changes']),
                      'source_verified': bool(source), 'origin_verified': bool(origin)}, indent=2))


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--source', type=Path)
    ap.add_argument('--origin', type=Path)
    args = ap.parse_args()
    verify(args.source, args.origin)

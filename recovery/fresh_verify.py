"""F-A append-only source projection checks; no production/service operations."""
import json
from delta_verify import DATA, REPO, changed, git, tree


def verify_fresh(source=None):
    doc = json.loads((DATA / 'fresh-map.json').read_text())
    net = json.loads((DATA / 'fresh-net-paths.json').read_text())
    base, tip = doc['baseline'], doc['runtime_tip']
    git(REPO, 'merge-base', '--is-ancestor', base, 'HEAD')
    git(REPO, 'merge-base', '--is-ancestor', tip, 'HEAD')
    before, delivered, head = tree(REPO, base), tree(REPO, tip), tree(REPO, 'HEAD')
    assert all(p.startswith('recovery/') for p in changed(delivered, head)), 'unmapped later runtime changes'
    previous = base
    if source:
        sequence = git(source, 'rev-list', '--first-parent', '--reverse',
                       doc['previous_source_freeze'] + '..' + doc['frozen_source']).decode().splitlines()
        assert sequence == [r['Source-Commit'] for r in doc['records']]
    for row in doc['records']:
        commit = row['Target-Commit']
        assert row['Target-Parent'] == previous
        assert git(REPO, 'rev-parse', commit + '^1').decode().strip() == previous
        old, new = tree(REPO, previous), tree(REPO, commit)
        assert changed(old, new) == {r['Target-Path'] for r in row['changes']}
        message = git(REPO, 'show', '-s', '--format=%B', commit).decode()
        assert 'taskId: kon3ad' in message
        assert 'Source-Commit: ' + row['Source-Commit'] in message
        assert 'Source-Paths:' in message
        assert row['no_op'] == (not row['changes'])
        if source:
            assert git(source, 'rev-parse', row['Source-Commit'] + '^1').decode().strip() == row['Source-Parent']
            a, b = tree(source, row['Source-Parent']), tree(source, row['Source-Commit'])
            assert {p for p in changed(a, b) if p.startswith('agentd/')} == {r['Source-Path'] for r in row['changes']}
            assert {p for p in changed(a, b) if not p.startswith('agentd/')} == {r['Source-Path'] for r in row['unrelated']}
            for r in row['unrelated']:
                assert (a.get(r['Source-Path']), b.get(r['Source-Path'])) == (r['before'], r['after'])
        for r in row['changes']:
            p, t = r['Source-Path'], r['Target-Path']
            assert p in message and t == p.removeprefix('agentd/')
            assert (old.get(t), new.get(t)) == (r['target_before'], r['target_after'])
            assert r['deleted'] == (t not in new) == (r['source_after'] is None)
            assert r['projection_reason']
            if source:
                assert (a.get(p), b.get(p)) == (r['source_before'], r['source_after'])
                if p in b:
                    assert new[t]['mode'] == b[p]['mode']
                    if p not in a:
                        assert new[t] == b[p]
        previous = commit
    adaptation = doc['adaptation']
    assert adaptation['parent'] == previous
    projected = tree(REPO, previous)
    for item in adaptation['commits']:
        assert item['parent'] == previous
        assert git(REPO, 'rev-parse', item['commit'] + '^1').decode().strip() == previous
        a, b = tree(REPO, previous), tree(REPO, item['commit'])
        assert changed(a, b) == {r['Target-Path'] for r in item['changes']}
        for r in item['changes']:
            assert (a.get(r['Target-Path']), b.get(r['Target-Path'])) == (r['before'], r['after'])
        msg = git(REPO, 'show', '-s', '--format=%B', item['commit']).decode()
        assert 'taskId: kon3ad' in msg and 'Source-Commit:' not in msg
        previous = item['commit']
    assert adaptation['commit'] == previous == tip
    assert changed(projected, delivered) == {r['Target-Path'] for r in adaptation['changes']}
    for r in adaptation['changes']:
        assert (projected.get(r['Target-Path']), delivered.get(r['Target-Path'])) == (r['before'], r['after'])
        assert r['reason']
    message = git(REPO, 'show', '-s', '--format=%B', tip).decode()
    assert 'taskId: kon3ad' in message and 'Source-Commit:' not in message
    names = {p for p in before.keys() | delivered.keys() if not p.startswith('recovery/')}
    assert names == {r['Target-Path'] for r in net}
    if source:
        old_source = tree(source, doc['previous_source_freeze'])
        frozen = tree(source, doc['frozen_source'])
    for r in net:
        p = r['Target-Path']
        assert (before.get(p), delivered.get(p), head.get(p)) == (r['baseline'], r['delivered'], r['delivered'])
        assert r['deleted'] == (p not in delivered) and r['reason']
        if source:
            assert (old_source.get(r['Source-Path']), frozen.get(r['Source-Path'])) == (r['source_previous'], r['source_frozen'])
    # Only verifier routing extends; all old refs/maps/results/READMEs are immutable.
    for p in doc['historical_metadata']:
        if p not in ('recovery/verify.py', 'recovery/rootfix_verify.py'):
            assert before[p] == head[p], p
    for ref, oid in doc['baseline_refs'].items():
        if ref == 'refs/heads/main':
            git(REPO, 'merge-base', '--is-ancestor', oid, 'HEAD')
        else:
            local = 'refs/remotes/origin/' + ref.removeprefix('refs/heads/')
            assert git(REPO, 'rev-parse', local).decode().strip() == oid
    readable = git(REPO, 'cat-file', '--batch-check', input=('\n'.join(doc['baseline_objects'])+'\n').encode()).decode()
    assert ' missing' not in readable and len(readable.splitlines()) == len(doc['baseline_objects'])
    print(json.dumps({'fresh_PASS': True, 'runtime_tip': tip, 'baseline': base,
                      'source_records': len(doc['records']), 'net_paths': len(net),
                      'baseline_objects': len(doc['baseline_objects']), 'source_verified': bool(source)}, indent=2))

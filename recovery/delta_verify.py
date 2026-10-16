"""Read-only verification of reviewed task-tab delta layered on accepted agentd.

All historical recovery metadata remains anchored to the accepted baseline;
this verifier checks the separate incremental projection and adaptation layer.
No checkout, temporary worktree, fetch, file deletion, model or service call.
"""
import difflib
import json
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / 'recovery'


def git(repo, *args, input=None):
    return subprocess.check_output(['git', '-C', str(repo), *args], input=input)


def tree(repo, rev):
    result = {}
    for entry in git(repo, 'ls-tree', '-rz', rev).split(b'\0'):
        if entry:
            info, path = entry.split(b'\t', 1)
            mode, kind, oid = info.decode().split()
            result[path.decode()] = {'mode': mode, 'blob': oid}
    return result


def changed(a, b):
    return {p for p in a.keys() | b.keys() if a.get(p) != b.get(p)}


def apply_source_delta(old, new, inherited):
    """Apply only changed source spans, retaining accepted unrelated adaptations."""
    a, b, out = old.splitlines(keepends=True), new.splitlines(keepends=True), inherited.splitlines(keepends=True)
    # Align source-before to accepted content first: the accepted report import
    # adaptation removes one line, shifting later source changes. Only apply a
    # span wholly in unchanged inherited context; reject overlap or ambiguity.
    matches = difflib.SequenceMatcher(None, a, out, autojunk=False).get_matching_blocks()
    operations = difflib.SequenceMatcher(None, a, b, autojunk=False).get_opcodes()
    replacements = []
    for tag, i, j, x, y in operations:
        if tag != 'equal':
            positions = {m.b + i - m.a for m in matches if m.size and m.a <= i and j <= m.a + m.size}
            assert len(positions) == 1, 'source change overlaps inherited adaptation or has ambiguous context'
            start = positions.pop()
            assert out[start:start + j - i] == a[i:j]
            replacements.append((start, start + j - i, b[x:y]))
    for start, end, replacement in reversed(replacements):
        out[start:end] = replacement
    return b''.join(out)


def verify_delta(source=None):
    doc = json.loads((DATA / 'delta-map.json').read_text())
    net = json.loads((DATA / 'delta-net-paths.json').read_text())
    base = doc['accepted_baseline']
    previous = base
    git(REPO, 'merge-base', '--is-ancestor', base, 'HEAD')
    # Preserve this source/adaptation layer at its original delivered revision.
    # Any later B1 mechanism fix is fully checked by rootfix_verify, not silently
    # folded into the source projection or its original adaptation records.
    rootfix_file = DATA / 'rootfix-map.json'
    revision = json.loads(rootfix_file.read_text())['failed_candidate'] if rootfix_file.exists() else 'HEAD'
    baseline, delivered = tree(REPO, base), tree(REPO, revision)
    if source:
        sequence = git(source, 'rev-list', '--first-parent', '--reverse',
                       doc['previous_source_freeze'] + '..' + doc['frozen_source']).decode().splitlines()
        assert sequence == [r['Source-Commit'] for r in doc['records']]
    for row in doc['records']:
        commit = row['Target-Commit']
        if source:
            assert git(source, 'rev-parse', row['Source-Commit'] + '^1').decode().strip() == row['Source-Parent']
            old, new = tree(source, row['Source-Parent']), tree(source, row['Source-Commit'])
            paths = changed(old, new)
            assert {p for p in paths if p.startswith('agentd/')} == {r['Source-Path'] for r in row['changes']}
            assert {p for p in paths if not p.startswith('agentd/')} == {r['Source-Path'] for r in row['unrelated']}
            for item in row['unrelated']:
                assert (old.get(item['Source-Path']), new.get(item['Source-Path'])) == (item['old'], item['new'])
        if commit is None:
            assert not row['changes']
            continue
        assert git(REPO, 'rev-parse', commit + '^1').decode().strip() == previous
        before, after = tree(REPO, previous), tree(REPO, commit)
        assert changed(before, after) == {r['Target-Path'] for r in row['changes']}
        message = git(REPO, 'show', '-s', '--format=%B', commit).decode()
        for key, value in [('Source-Repo', row['Source-Repo']), ('Source-Commit', row['Source-Commit']), ('taskId', doc['taskId'])]:
            assert key + ': ' + value in message
        for item in row['changes']:
            p, t = item['Source-Path'], item['Target-Path']
            assert t == p.removeprefix('agentd/')
            assert p in message and t in message
            assert (before.get(t), after.get(t)) == (item['target_before'], item['target_after'])
            assert item['deleted'] == (item['source_after'] is None)
            if source:
                assert (old.get(p), new.get(p)) == (item['source_before'], item['source_after'])
                if item['source_after'] is None:
                    assert t not in after
                elif item['source_before'] is None:
                    assert after[t] == new[p]
                else:
                    expected = apply_source_delta(
                        git(source, 'show', row['Source-Parent'] + ':' + p),
                        git(source, 'show', row['Source-Commit'] + ':' + p),
                        git(REPO, 'show', previous + ':' + t))
                    assert git(REPO, 'hash-object', '--stdin', input=expected).decode().strip() == after[t]['blob']
                    assert after[t]['mode'] == new[p]['mode']
        previous = commit
    adaptation = doc['adaptation']
    assert adaptation['parent'] == previous
    assert git(REPO, 'rev-parse', adaptation['commit'] + '^1').decode().strip() == previous
    projected, adapted = tree(REPO, previous), tree(REPO, adaptation['commit'])
    assert changed(projected, adapted) == {r['Target-Path'] for r in adaptation['changes']}
    for item in adaptation['changes']:
        p = item['Target-Path']
        assert (projected.get(p), adapted.get(p)) == (item['projected'], item['adapted'])
    names = {p for p in baseline.keys() | delivered.keys() if not p.startswith('recovery/')}
    assert names == {r['Target-Path'] for r in net}
    if source:
        source_previous = tree(source, doc['previous_source_freeze'])
        source_frozen = tree(source, doc['frozen_source'])
    for item in net:
        p = item['Target-Path']
        if source:
            source_path = p if p in ('platform_paths.py', 'test_platform_paths.py') or p.startswith('portable/') else 'agentd/' + p
            assert item['Source-Path'] == source_path
            assert (source_previous.get(source_path), source_frozen.get(source_path)) == (item['source_previous'], item['source_frozen'])
        assert (baseline.get(p), projected.get(p), delivered.get(p)) == (item['accepted'], item['projected'], item['delivered'])
        assert adapted.get(p) == delivered.get(p), 'runtime change outside adaptation'
        assert item['deleted'] == (p not in delivered)
        if not item['reason']:
            assert baseline.get(p) == delivered.get(p), 'unexplained baseline regression'
    for line in doc['baseline_refs'].splitlines():
        oid, ref = line.split()
        actual_ref = 'refs/remotes/origin/' + ref.removeprefix('refs/heads/')
        if ref == 'refs/heads/main':
            git(REPO, 'merge-base', '--is-ancestor', oid, 'HEAD')
        else:
            assert git(REPO, 'rev-parse', actual_ref).decode().strip() == oid
    print(json.dumps({'delta_PASS': True, 'verified_revision': revision, 'source_records': len(doc['records']),
                      'net_paths': len(net), 'changed_runtime_paths': len(changed(baseline, delivered) & names),
                      'source_verified': bool(source)}, indent=2))

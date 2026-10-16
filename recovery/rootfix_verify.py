"""Read-only B1 mechanism layer checks; historical source mappings stay intact."""
import json
from delta_verify import DATA, REPO, changed, git, tree


def verify_rootfix(source=None):
    doc = json.loads((DATA / 'rootfix-map.json').read_text())
    delta = json.loads((DATA / 'delta-map.json').read_text())
    old_net = json.loads((DATA / 'delta-net-paths.json').read_text())
    net = json.loads((DATA / 'rootfix-net-paths.json').read_text())
    base, fix = doc['failed_candidate'], doc['mechanism_commit']
    assert doc['frozen_source'] == delta['frozen_source']
    assert doc['accepted_baseline'] == delta['accepted_baseline']
    git(REPO, 'merge-base', '--is-ancestor', doc['accepted_baseline'], base)
    git(REPO, 'merge-base', '--is-ancestor', fix, 'HEAD')
    assert git(REPO, 'rev-parse', fix + '^1').decode().strip() == base
    before, fixed, delivered = tree(REPO, base), tree(REPO, fix), tree(REPO, 'HEAD')
    assert changed(before, fixed) == {r['Target-Path'] for r in doc['changes']} == {'report.py', 'test_wait_state.py'}
    for r in doc['changes']:
        p = r['Target-Path']
        assert (before.get(p), fixed.get(p)) == (r['before'], r['after'])
        assert r['deleted'] == (p not in fixed)
        assert r['reason']
    message = git(REPO, 'show', '-s', '--format=%B', fix).decode()
    assert 'taskId: ' + doc['taskId'] in message
    assert 'Source-Commit:' not in message, 'mechanism fix is not a new source projection'
    assert doc['frozen_source'] in message
    assert all(p.startswith('recovery/') for p in changed(fixed, delivered)), 'unmapped later runtime change'
    # Original projection/adaptation metadata is immutable, including modes.
    for p in doc['immutable_metadata']:
        assert before[p] == delivered[p], p
    names = {p for p in before.keys() | delivered.keys() if not p.startswith('recovery/')}
    assert len(net) == len(names) and {r['Target-Path'] for r in net} == names
    inherited = {r['Target-Path']: r for r in old_net}
    if source:
        frozen = tree(source, doc['frozen_source'])
    for r in net:
        p = r['Target-Path']
        old = inherited[p]
        for key in ('Source-Path', 'accepted', 'source_previous', 'source_frozen', 'projected'):
            assert r[key] == old[key], (p, key)
        assert r['failed_candidate'] == old['delivered'] == before.get(p)
        assert r['delivered'] == fixed.get(p) == delivered.get(p)
        assert r['deleted'] == (p not in delivered)
        assert r['mechanism_reason'] or before.get(p) == delivered.get(p)
        if source:
            assert frozen.get(r['Source-Path']) == r['source_frozen']
    assert delivered['wait_state.py'] == before['wait_state.py'], 'no protocol change'
    print(json.dumps({'rootfix_PASS': True, 'mechanism_commit': fix,
                      'failed_candidate': base, 'frozen_source': doc['frozen_source'],
                      'net_paths': len(net), 'changed_runtime_paths': len(changed(before, fixed)),
                      'source_verified': bool(source)}, indent=2))

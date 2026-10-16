#!/usr/bin/env python3
"""Emit a standalone shell regression from actual source, suitable for Bash 3.2.

Run: python recovery/make-shell-fixture.py > /new/path/fixture.sh
     /bin/bash /new/path/fixture.sh
Fixtures use only a fresh /tmp root, retained for inspection; no daemon starts.
"""
from pathlib import Path
import shlex

repo = Path(__file__).resolve().parent.parent
print('''#!/bin/bash
set -eu
printf 'Bash: %s\\n' "$BASH_VERSION"
BASE=$(mktemp -d /tmp/agentd-shell.XXXXXX)
REPO="$BASE/caller workspace/checkout with spaces"
mkdir -p "$REPO"
printf 'Retained fixture: %s\\n' "$BASE"
''')
def emit(name, text):
    print('cat > "$REPO/' + name + '" <<\'AGENTD_FIXTURE_CONTENT\'')
    print(text)
    print('AGENTD_FIXTURE_CONTENT')
emit('platform_paths.py', (repo / 'platform_paths.py').read_text())
for name, var in [('loop.sh', 'RUNNER'), ('scheduler-loop.sh', 'SCHEDULER')]:
    source = (repo / name).read_text()
    prefix = source[source.index('# Resolve real and symlink'):source.index('\n' + var + '=')]
    emit(name, 'set -u\n' + prefix + '\nprintf "%s\\n" "$MODULE_DIR" "$WS"\n')
    print('NAME=' + shlex.quote(name))
    print('''ln -s "caller workspace/checkout with spaces/$NAME" "$BASE/linked $NAME"
for ENTRY in "$REPO/$NAME" "$BASE/linked $NAME"; do
    GOT=$(PLATFORM_WORKSPACE= /bin/bash "$ENTRY")
    WANT=$(printf '%s\\n%s' "$REPO" "$BASE/caller workspace")
    [ "$GOT" = "$WANT" ]
    GOT=$(PLATFORM_WORKSPACE="$BASE/override with spaces" /bin/bash "$ENTRY")
    WANT=$(printf '%s\\n%s' "$REPO" "$BASE/override with spaces")
    [ "$GOT" = "$WANT" ]
done
(cd "$BASE"; PLATFORM_WORKSPACE= /bin/bash "caller workspace/checkout with spaces/$NAME")
''')
source = (repo / 'scheduler-loop.sh').read_text()
assembly = source[source.index('# The caller supplies'):source.index('while :; do')]
emit('args.sh', 'set -u\n' + assembly + '\nprintf "%s\\n" "$#" "$@"\n')
print('''unset AGENTD_MAX_CONCURRENT
[ "$(/bin/bash "$REPO/args.sh")" = 0 ]
[ "$(AGENTD_MAX_CONCURRENT= /bin/bash "$REPO/args.sh")" = 0 ]
[ "$(AGENTD_MAX_CONCURRENT=10 /bin/bash "$REPO/args.sh")" = "$(printf '2\\n--max-concurrent\\n10')" ]
printf '%s\\n' 'PASS: real/symlink/relative entries, spaces, helper override/default, concurrency argv'
''')

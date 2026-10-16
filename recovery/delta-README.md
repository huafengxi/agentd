# Reviewed task-tab source increment — rd6-a-delta-182939

Candidate source only. No production/caller activation; no pi-wrap change or pin
approval is implied. Existing accepted history and recovery evidence are retained.

## Frozen source and layered provenance

- Accepted agentd baseline: `9625bdc15f853e49a6312d20dc429e23330e3d89`.
- Previous platform freeze: `4ac4631fbdbf27f3c400805b233dc1d7a9892d46`.
- New platform freeze: `1f650e88357b29908526245f6b7ccae2d5540464`.
- The complete first-parent interval contains one commit. It changes three owned
  paths: `agentd/report.py`, `agentd/wait_state.py`, `agentd/test_wait_state.py`.
  The fourth path, `pi-wrap/pi-rpc-wrap.py`, belongs to the existing pi-wrap task;
  it is recorded as unrelated, not cherry-picked into this repository.
- Exact projection: `78b87f722bb9220dd7297de0268e79063e09b823`.
  Source deltas preserve accepted unrelated realpath imports. New wait_state and
  test blobs/modes are exact source copies at this stage. No deletion occurs.
- Separate standalone adaptation: `f98d917a0ce8c1b8d353da135a1ebaf2d887a547`.
  Only report.py and test_wait_state.py change: the displayed reply command finds
  agentctl beside the real report source; tests use explicit wrapper input,
  same-directory helper and the accepted src-based page widget. A path containing
  spaces and execution of the actual displayed ref command are tested.

`delta-map.json` records source/target commits, all changed paths, before/after
blobs/modes/deletion flags, taskId, unrelated paths and adaptation reasons.
`delta-net-paths.json` covers all non-recovery paths against the accepted tree,
source snapshots, projection and delivery, including unchanged inherited content.
Only the three owned paths change relative to accepted agentd; previous runtime,
helper, shell, portable, scheduler/runner policy and widgets do not regress.

Historical source-map/adaptations/net-paths/test-results remain immutable records
for the accepted baseline. `verify.py` now verifies that layer at its accepted
revision and then runs `delta_verify.py` against the increment and current HEAD.
The latter reconstructs the source delta on accepted content, checks first-parent
coverage, exact modes/blobs/deletions, separate adaptation and all inherited paths.
Its source-span application aligns unchanged inherited context (including the
accepted one-line import shrink), then rejects overlaps or ambiguous context
instead of inventing a merge policy. No runtime behavior is affected.

## Shared wait protocol and consumer interface

`from wait_state import CurrentWaits, DIALOG_METHODS` is available from this
candidate's root. DIALOG_METHODS is select/confirm/input/editor. The wrapper is
still the **sole production writer** of wait-state.json. report is a read-only
consumer. Identity remains task + (pid, procStart); old producers without a
snapshot give no hint. Current structured ask ref/reply and current dialogs,
response/deadline/tool-end/round completion and remaining-time attach replay are
unchanged from the reviewed source. No second protocol, disabled waiting/replay,
implicit unified-repository lookup or automatic guard is introduced.

## Reproduce (no model, fresh isolated roots)

First export the pi-wrap Python sources at the frozen platform commit to your
own new test-input directory. Do not supply its sibling agentd tree. Export the
actual ask driver, ask-user-child.ts and its core.ts dependency from main source
commit `cf97b1145e50967aac6d2f5a5b58e0bcc6d8157d`, keeping relative paths. These
are caller-owned test inputs, not agentd runtime assets. The real driver uses the
installed Node/npm/jiti/typebox; do not download/install anything for this test.
Use the site's actual read-only marked.min.js renderer.

```sh
# Set TMPDIR and HOME below a freshly-created short isolated base (Unix socket
# path length matters), and PATH to the existing Python/Node/npm tools.
AGENTD_TEST_WRAP=/your/new/input/pi-wrap/pi-rpc-wrap.py \
ASK_WAIT_DRIVER=/your/new/input/agentd/tests/ask-wait.test.mjs \
TASK_MARKED_JS=/your/read-only/site/marked.min.js \
python3 -B test_wait_state.py

python3 -B recovery/run-tests.py
python3 -I -S -B portable/test_portable.py
python3 -B recovery/verify.py --source /your/platform-source.git --origin /your/agentd.git
```

The explicit wait run passes **14 tests without skips**: real page active widget,
actual HTTP report adapter, site Markdown/HTML, true TS ask producer -> frozen
wrapper -> candidate snapshot -> page -> displayed exact-ref reply -> clear;
TS timeout with historical ask retained; no-browser dialog/attach/response;
all four dialog types, timeout/notice/abort distinctions; current replay vs ring
history/eviction; process incarnation, terminal/stale/unreliable-time filters and
unrelated inflight round settlement. Wrapper module imports assert candidate
proto identity and the exact same CurrentWaits class. Subprocesses explicitly
receive AGENTD_DIR pointing to this candidate, with no copied protocol fallback.
Fixture removal uses safe_rmtree only below the self-created base.

The ordinary runner discovers ten root test scripts (including wait_state),
50 core e2e checks and ten portable tests; all commands pass. Without explicit
caller dependencies, the ordinary wait run has five integration skips, separately
covered by the explicit 14-test run above. Core e2e retains its documented
cross-repo/live-pi exclusions and three caller-context skipped subassertions.
Report slots: 38 passed/0 failed. Scheduling: 8 tests OK. Portable still runs real
POSIX demo/doctor/ctl, build/unpack, -I -S -B and offline install/download traps.

No helper/shell change was made, so no new Bash3.2 matrix is claimed. The accepted
baseline's independent review had already supplemented its original Bash3.2 gap.
These are Linux isolated tests, not macOS/multi-host/production/browser activation
acceptance. Source after the freeze, if any, requires separate provenance/review;
the frozen wrapper test input is not a substitute for a reviewed pi-wrap pin.

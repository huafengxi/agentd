# B1 effective reply root — rd6-a-rootfix-185503

Candidate-only bounded repair. No deployment, caller/pin activation, original
platform edit, protocol copy, disabled wait/replay, new guard or fallback.
Implementation tests are not an independent review gate.

## Layer and exact scope

- Accepted agentd: `9625bdc15f853e49a6312d20dc429e23330e3d89`.
- Failed task-tab candidate: `cf91bbf164df7dd8424480fff8c1a3520789c44e`.
- Frozen platform source remains `1f650e88357b29908526245f6b7ccae2d5540464`.
- The three-path source projection and original standalone adaptation remain
  unchanged in history and in delta-map/delta-net-paths. No new Source-Commit
  is asserted: this is a mechanism repair of independent review B1, not a new
  source projection or an expansion of the frozen source range.
- Separate mechanism commit: `ec653531ab8d2e893fd9a435ea9446be9f5e406a`.
  Only `report.py` and `test_wait_state.py` change. report retains mode 100755;
  test retains 100644. No runtime path is created/deleted and no mode changes.
  wait_state.py is unchanged and still matches the frozen source.

`build_report` passes its effective workspace root into `wait_cell`. The displayed
command uses the adjacent real-source agentctl, then a shell-quoted **absolute
root as the global --root argument before send**. This also binds a default-root
page to its selected root if the answerer has a conflicting environment/cwd.
No search, retry, root guessing, environment mutation or production fallback is
added. Existing precise ask-ref and file-body semantics remain unchanged.

## Recovery records and read-only verification

`rootfix-map.json` records the failed/accepted pins, frozen input (not a new
source commit), separate mechanism commit, exact before/after blobs, modes,
deletions and reasons. `rootfix-net-paths.json` covers all 39 non-recovery paths
with original source/accepted/projection fields, failed-candidate delivery and
current delivery. Only report/test differ from the failed candidate; every
other runtime/helper/shell/portable path remains byte/mode-identical.

The historical JSON/README evidence is not rewritten. `delta_verify.py` checks
its original source/adaptation layer at the failed candidate; `rootfix_verify.py`
then checks the complete new layer, direct parentage, exact changed paths,
immutable metadata, inherited content and that any later commits only affect
recovery. `verify.py` runs all three layers, including original refs/object
closure/fsck and (when requested) actual origin main equality.

```sh
python3 -B recovery/verify.py --source /your/platform-source.git \
  --origin /your/agentd.git
```

This is continuation/preservation verification, not a repeated six-repository
or dual-host history-recovery audit. Original refs are retained, accepted and
failed candidates are ancestors, and main only advances by fast-forward.

## Regression and reproduction (new isolated roots, no model)

The new `WaitTests.test_report_root_reply_entrance` executes the real report CLI
with a selected root, extracts the **actual displayed command**, replaces only
the body-file placeholder and executes real agentctl from an unrelated cwd.
Three subcases cover explicit relative --root with a clean environment, explicit
--root with a conflicting environment root, and default-root report with a
conflicting reply environment. All roots have same-task decoys; paths contain
spaces and the selected external root also contains a single quote. The test
requires exact ref/body only in the selected inbox, no decoy reply, actual wait
cleared before tool-end, and removal on the next report. The original independent
B1 script (absolute explicit root, separate default root) also passes unchanged
in behavior; its success condition was strengthened to require wait cleared.

The new regression fails in all three subcases with the old report implementation
(rc=0 replies go to another tree), and passes with the fixed report.

To run only this regression without cross-repo dependencies:

```sh
# HOME and TMPDIR must be under a fresh self-created short fixture base.
python3 -B test_wait_state.py WaitTests.test_report_root_reply_entrance
```

For the complete wait suite, export only pi-wrap Python files from the exact
frozen platform source into your own test-input directory (no sibling agentd).
Supply the actual read-only main-repository ask driver and site marked input:

```sh
AGENTD_TEST_WRAP=/your/input/pi-wrap/pi-rpc-wrap.py \
ASK_WAIT_DRIVER=/your/read-only/main/pi-core/agent/extensions/agentd/tests/ask-wait.test.mjs \
TASK_MARKED_JS=/your/read-only/main/w/ext/markdown/deps/marked.min.js \
python3 -B test_wait_state.py

python3 -B test_report_slots.py
python3 -B test_scheduling.py
python3 -B test_web_report.py
python3 -I -S -B portable/test_portable.py
python3 -B test_platform_paths.py
```

Use an environment cleared of production AGENT_*/PLATFORM_WORKSPACE values and
existing Python/Node tools on PATH. Keep HOME/TMPDIR under a newly created short
base because wrapper AF_UNIX paths have a length limit. The suite asserts that
the explicit frozen wrapper imports this candidate's proto/CurrentWaits, and
subprocesses receive AGENTD_DIR explicitly; no drifting production agentd or
sibling repository is used. CurrentWaits/DIALOG_METHODS are also directly imported
under -I -S -B. No model, installation/download or production inbox/socket call.
Fixture/socket lifecycle cleanup is limited to existing tests' self-created
roots; no separate GC, deletion experiment or outer-root cleanup is performed.

## Actual results and limits

`rootfix-test-results.json` records full commands, isolated environment/base,
actual input hashes, durations, return codes and log locations. On dev/Linux:

- Wait suite: **15 tests OK, no skips**, including actual file ask/TS producer,
  frozen wrapper, dialog attach/replay, page/marked/HTTP adapter, answer removal,
  old incarnation, already-answered, timeout/terminal/stale and entry regressions.
- report slots: **38 passed / 0 failed**; scheduling **8 OK**;
  web_report **7 OK**; portable **10 OK**; platform_paths **3 OK**.
- Direct CurrentWaits/DIALOG_METHODS interface import: PASS.
- Independent B1 reproduction adapted only to the new self-owned candidate path:
  selected ref received, default inbox empty, selected_still_waiting=false.

No helper/shell changed: no conditional Bash3.2 rerun is claimed. The root/space/
symlink/shell-root contract is still tested by test_platform_paths and portable.
No full runner/core e2e, pi-wrap candidate pin integration, macOS/multi-host,
production/browser/model activation is claimed by this bounded repair.
Independent review and dispatcher acceptance are still required before any P
continuation. D5 remains closed; D6 adds no guard; P1/P6 are not complete.

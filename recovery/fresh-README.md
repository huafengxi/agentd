# F-A source continuation — taskId kon3ad

Unactivated agentd candidate only. This is not a new P/S combination PASS,
P3 adapter, deployment, or approval to change any caller/dependency pin.

## Fixed source history

Starting accepted A: `ca35c53c6bd9c437615058126c5bcd2f97de2801`.
Previous platform source freeze: `1f650e88357b29908526245f6b7ccae2d5540464`.
Frozen source: `1156870f3bf23d00bd8ced0717fc63331e08e54b`.

Three first-parent source commits are projected in order:

1. `1090df79741716fac97c8f9e1b75e361d6e3f9c0`: lifecycle acceptance tests and docs.
2. `4dbd3ca779aae0ae9e7fb98182d77f826d3be436`: remove persistent wait observation and task-tab wait guesses.
3. `1156870f3bf23d00bd8ced0717fc63331e08e54b`: read-only current-branch unresolved tool observations and ten-column active table.

`fresh-map.json` enumerates each source's actual agentd paths, modes, before/after
source and target blobs, unrelated paths, and target commits. No source in this
three-commit interval is a no-op. This is a three-way path projection, not raw
platform file replacement: source deletions win for obsolete wait UI/tests,
while inherited standalone imports/widget source paths/read-only boundary stay.
All messages carry Source-Commit, Source-Paths and taskId. Two separate local
adaptation commits follow the source projections. `fresh-net-paths.json` covers
all forty non-recovery terminal paths, including unchanged assets and modes.

No historical refs/maps/results/README records were rewritten. Only verifier
routing was extended: rootfix_verify validates its original exact accepted A
terminal tree; fresh_verify checks the complete subsequent source/adaptation
chain, current terminal runtime tree, baseline refs and 540 original reachable
objects. verify.py invokes historical, delta, rootfix, then fresh checks. It
still checks actual origin main when --origin is provided.

## Interfaces and meaningful inherited contracts

- `wait_state.DialogRequests()` replaces `CurrentWaits(home, task)`;
  `DIALOG_METHODS` remains select/confirm/input/editor. `event`, `sent`, `dialogs`
  maintain only in-memory socket-attach dialog replay/settlement. No constructor
  root/identity, persistent snapshot writer, `current_waits`, ask observer or
  tool_execution_update partial-result observer remains.
- `report.current_tools(adir, pid_doc, now)` reads only bounded session/session.jsonl
  v3 current persisted branch. It filters by generation startedAt, follows
  parentId, pairs toolCall IDs/results, rejects invalid metadata/partial input,
  rechecks pid identity and caps bytes/line bytes/entries/display count. It is
  neither execution truth nor wait proof. Ten columns include current tools,
  not the deleted extra wait column. Report never opens/updates old wait-state.json.
- All root/helper, protocol, scheduler/provider, transport and portable code
  outside the mapped runtime paths remains unchanged. Removed wait_cell's
  generated root-specific reply link has no UI successor because the wait UI
  was intentionally deleted. Its meaningful rootfix survives as real agentctl
  --root/exact-ref reply from unrelated cwd with conflicting environment,
  preserving the unrelated nonblocking ask, using fixed TS producer + fakepi.
- Old P still imports CurrentWaits. It must continue selecting the permanent old
  A object; do not add compatibility persistence or silently move its pin.
  F-P must separately select this new A revision, require DialogRequests and
  DIALOG_METHODS, project its fixed source deletion chain, update its dependency
  checks/recovery continuation mapping, and test both layouts with fakepi. F-C
  then freezes caller and full combination. Neither gate is passed here.

## Validation and isolation

Use the same-repo recovery/isolated-test.py for stdlib test files, fresh HOME and
TMPDIR, and a synthetic state workspace/agents/inbox/host-map. All roots are
new self-owned fixtures, no production sockets, synchronization, GC, or models.
For integration-bearing A tests explicitly provide:

- AGENTD_TEST_WRAP: exported platform wrapper at the frozen source (test input,
  not the standalone P repository or an adapted P candidate).
- ASK_WAIT_DRIVER: exported caller tests/ask-wait.test.mjs at
  `fc5d66a08ff05122a0fa01e781903f1eed54a9a5` (includes producer deletion
  `967cf7d53eb5cca9af645813535a9661b1b3e5d9`).
- TASK_MARKED_JS: exported marked renderer; NATIVE_RPC_SOURCE: explicit read-only
  native rpc-mode.js helper input. The latter extracts createDialogPromise into
  a mock node process, never launches real pi/model.

Wrapper imports are asserted to use this A's actual proto/DialogRequests.
These tests cover A's interface with frozen-source fixtures, **not full fixed P
integration**. Without inputs they skip applicable wrapper/native/HTML cases.

Current results and honest earlier harness failures are recorded in
fresh-test-results.json. Do not treat inherited PASS evidence as new execution.
Core runs omit PLATFORM_WORKSPACE to test default-root semantics; execute a
clean export inside a new synthetic workspace, with caller constants supplied
read-only. Full standalone core uses --core (never --integration/--live-pi).
The old recovery/run-tests.py intentionally supplies no external inputs and
therefore is not a substitute for the explicit-input F-A validation.

Reproduce provenance with:

```sh
python3 -B recovery/verify.py --source /path/to/fixed-platform-repo --origin /path/to/agentd.git
```

The task report supplies the final pushed pin, exact execution environments,
commands, retained raw failure logs, and clean-export rerun evidence. No live
service/profile/version/launcher change is included.

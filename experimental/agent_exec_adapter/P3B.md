# P3b resident / auto / controls / original notifications — unwired sandbox

Task **a41wn0**, parent **6ggidb**. User approved only this minimal P3b sandbox
(`agents/task/6ggidb/p3b-approval-20261017.md`). Nothing imports it from an active
runner/loop. No daemon, production wiring, new host, model session, S change or
copy of S supervision is provided. D5 closed; D6 adds nothing. This is not a
claim of the full legacy agent/command/persona matrix being compatible.

## Current dependency and source

A baseline is `e73c21579d10d9d4053c2ebbee0d4e7c6c8a9b6e`. Current
`runtime-dependency.json` and `adapter.S_REVISION` explicitly select fixed S
**4f99b25d191f61734b6b1ef89b456014413aad92** from the serviced origin (in this
sandbox `/home/yuanqi.xhf/git/serviced.git`), not ambient installed runtime.
`adapter.load_runtime` checks every declared source hash, refuses pyc, and loads
the clean explicit export under `_p3a_s_<full_revision>` (historical package
prefix is not a different S pin). `node/state/adapter-imports.json` records real
`runtime/{__init__,contract,prepare,kernel,lifecycle}.py` paths. Supply a read-only
fixed `git archive`, run `-B`; no fallback to production source/root.

The old P3a commit remains usable with its old S e4b79dce… export. This new
commit runs the retained P3a module/tests against new S as well. S legacy A0
`9625bdc15f853e49a6312d20dc429e23330e3d89`, P and all other consumers are untouched.

## Exact lifecycle mapping (derived from original predicates)

The authoritative inputs are `task_state.is_resident` (only literal true),
`runner.Runner.service`'s finalize block and auto block, `proto.CRASH_STATUSES`.
The expression is `policy == 'one-shot' or (policy != 'auto' and not resident)`.
`p3b.lifecycle_mapping` tests that same expression, not a provider success rule.

| resident | restartPolicy | runtime kind / policy | natural exit agent final | automatic replacement |
|---|---|---|---|---|
| false / absent | absent | Task / no retry | true | none |
| false / absent | manual | Task / no retry | true | none |
| false / absent | one-shot | Task / no retry | true | none |
| false / absent | auto | Service / always | false | S only, exit0 **and** nonzero |
| true | absent | Service / manual | false | none |
| true | manual | Service / manual | false | none |
| true | auto | Service / always | false | S only, exit0 **and** nonzero |
| true | one-shot | **Task / no retry** | **true** | none (original special case) |

Scope: `task/<id>` only, finite new synthetic manifest, exact host, no profile,
only declared cooperative argv under literal `p3a-fixture <label>` command binding
(the string is NOT executed by shell). Unknown policies, nonboolean resident,
undeclared commands, persona/model/profile, bot/queue families and arbitrary
legacy workdirs are explicitly rejected. There is no silent on-failure fallback,
Task auto retry, general compatibility claim, or alternate supervisor.

`spec.workdir` must explicitly equal `p3b.initial_workdir(...)`: initial Task
`work/Task/<id>/a1/1`, Service `work/Service/<id>/service/1`. Runtime owns distinct
work directories per replacement; this synthetic binding is not transparent
preservation of an arbitrary legacy cwd. Task agent generations map to explicit
attempts (`a1`, `a2`, …; runtime generation **always 1**); an unused pre-start
attempt may be canceled before external restart, so agent gen1 may be attempt a2.
Service agent gen = S generation under the same stable runtime id. Agent final
is absorbing: no pid refresh, rerun, clear or restart after final. Runtime results
remain immutable. Unknown wait => stale/127 projection, never fabricated exitCode.

Original scheduler registration/prompt order, host/target, dependency/provider,
resources/slots stay unchanged. Original resident predicate exempts **both**
resources and slots. For nonresident auto, S backoff is still owned/nonfinal:
pid status remains `running` as an **ownership reservation**, not a claim that
the old process is alive; `pid.runtime.servicePhase=backoff` and runtime old
result contain exit/wait facts. This prevents a scheduler resource/slot release
between generations. `pid.log` archives actual old terminal facts, not a fake
wait. Identity/lease/group unknown also remains nonfinal/occupied.

S alone schedules always backoff `.1/.2/.4/.4...` seconds using its existing
bounded policy and explicit `tick_service`. Adapter never computes a restart
schedule, spawns, calls kill/killpg, polls/waits a Popen, or runs Runner.auto.

## Single-owner / writer graph

```text
real agentctl register-json -> original spec/prompt
original scheduler (target, dependencies, resources, slots, resident exemption)
    -> actual enable (scheduler sole writer)
    -> P3b Adapter explicit Runtime methods
         Runtime.accept/start/observe_task, rerun_task (external controls only)
         Runtime.submit/service/read_run/tick_service, control
                 |
fixed S Runtime/Kernel ONLY: prepare, Popen/setsid, wait/observe, group/identity,
    TERM/KILL, node flock, per-run lease, generation/attempt run + result writers
                 |
Adapter.project ONLY -> original Runner.write_pid atomic primitive
Adapter agent policy -> archive_gen, clear backup/truncate/claim, inject,
    pending_reqs/write_ack, resolve_reaper/_ask_inbox_env/notify_tick
original task_state/provider -> original scheduler -> needs successor
```

Runner exists only as a pure agent helper object. Its
`service/spawn/observe/kill_proc/do_stop/do_restart/do_clear/handle_controls`
are forbidden sentinels during real chain tests, including the bounded real CLI.
Audit stacks require all payload Popen and killpg to originate in S kernel.
Workspace immutable binding and S node flock fence the sole pid writer;
heartbeat has the real adapter owner pid/procStart. Runtime knows neither JSONL,
agent inbox, final, reports nor provider/reaper policy.

## Controls and conservative partial-effect contract

All tested requests are genuine `agentctl control` / `cancel` `.req` files and
original `Runner.write_ack` files at the **exact request id**. Runtime controls
have deterministic requestId from the whole agent request, actual kind/attempt,
and explicit expected S generation. Optional expected agent generation is
validated separately. Original CLI lacks that field, so its timestamp is bound
against the current agent startedAt: a delayed request older than its successor
is noop, absent/unparseable timestamp without generation is rejected, never a
license to signal unknown identity. Repeated exact id is skipped by the original
ack primitive. A completed policy journal restores a lost ack without repeating
effects. Service controls are processed before an autonomous backoff tick so a
valid stop is not overtaken by a replacement started by that same adapter tick.

- Pre-start stop: Task accepted without execution -> S cancel; Service submitted
  **offline** (no run/lease/process) -> final gen0. No harness enable/final.
- Pre-start clear: noop, no pid/session created. Pre-start external restart retains
  the original permission to start even before enable. A Service is first offline,
  then explicitly submitted online/new version; a Task uses explicit rerun of the
  unused canceled attempt. This is external control, not scheduler policy bypass
  added for autonomous admission.
- Running stop: S must confirm old group/lease closure before final/ack. A natural
  already-terminal resident stop finalizes with noop; queued/running stops applied.
  Pending/cleanupUnknown is not acknowledged as stopped.
- Nonfinal restart/clear: stop through S first, preserve old S result and append
  agent pid.log; restart may inject via original helper and writes resumed gen.
  Clear then uses **original** `_backup_and_clear_session` (copy backup, truncate,
  `_claim_origin`), only the current task's session, then explicit S replacement.
  No resumed on clear; inbox/ack/spec/enable are untouched. Runtime never reads
  JSONL. The claim test binds a synthetic replica role label to the fixture's
  actual current gid, then executes real own-file chown; production replica
  group transfer/permissions are not claimed. Backup callback tests inspect real stopped group/lease **before** backup.
- Final stop noop; final restart/clear rejected. Natural Task finalization still
  precedes external controls as in original Runner; a nonfinal one-shot **running**
  control may replace it, never a final one-shot.

`state/adapter/<id>-p3b.json` is agent policy state, not a second result store.
It is persisted pending **before** control effects. Interrupted admission,
unknown stop, failed replacement, partial clear/inject/storage or ambiguous
runtime receipts block further autonomous progress / later controls for that
participant. They are not replayed on re-open. A clear backup error emits the
original rejected ack but retains this conservative pending boundary; it never
starts a successor using possibly old history. A replacement failure retains S's
failed generation/result and a nonfinal conservative old projection, no applied
ack. Service prepare failure before head creation retains its failed receipt and
pending admission (no fabricated pid). Operator inspection is required; this
experiment intentionally supplies **no** new recovery transaction mechanism,
cross-node takeover, GC or automatic repair. These are disclosed availability
limits, not a claim of equivalence for all partial-crash windows.

## Notifications / provider / communication

All terminal event/report/no-report/cancel classification, none sentinel, legal
live recipient and inactive/missing reaper fallback with note, retry, mark dedupe,
cold replay guard are original Runner helpers. Adapter doesn't copy success
standards. `task_done` / `task_failed` / `task_canceled` are notification signals,
not provider success and never independent review PASS. Exit0 without nonempty
report remains idle; true nonzero remains failed even with report; stale127 with
nonempty report retains success. A stop with already-delivered report retains
original priority; a killed/no-report stop is canceled, not idle/no_report warn.
The original envelope-before-mark crash window is at-least-once, so one duplicate
may be retried; it is NOT exactly-once delivery.

Nonresident env uses original ask routing; resident env sets AGENTD_RESIDENT and
omits AGENTD_TASK/ask-dispatcher task env exactly like the old Runner. Real CLI
ask->answer tests retain inbox/ref/via/deliver. No receiver/TS injection or model
is run, so no strong claimedTs ack, blocking ask_dispatcher wait, native dialog,
prompt/init-ok/recv-armed/settle/model-error handshake is claimed. No new waiting
observation files or direct interactive reaper liveness bridge are added.

## Invocation, reproducible test inputs and evidence

Use a fresh isolated workspace/agents/host-map, source Git, manifest, runtime root,
HOME/TMPDIR/Git config and foreground Linux cooperative Python payload. Same
finite manifest schema as README, but `experiment=p3b-lifecycle-fixtures-v1` and
registered resident/restartPolicy combinations above. No production root/socket,
model, sync/GC, service/Makefile/profile change. All relative placeholders below
mean new absolute synthetic paths, never the production workspace.

```sh
PYTHONDONTWRITEBYTECODE=1 /absolute/miniconda3/bin/python -B \
  -m experimental.agent_exec_adapter.p3b \
  --sandbox-root /ABS/NEW-SANDBOX --workspace /ABS/NEW-SANDBOX/workspace \
  --runtime-root /ABS/NEW-SANDBOX/node --runtime-source /ABS/fixed-S-export \
  --manifest /ABS/NEW-SANDBOX/manifest.json --seconds 2

# env -i with private HOME/TMPDIR/cwd/PATH/Git config/PYTHONPATH; output must not exist
P3A_TEST_OUTPUT=/ABS/NEW-OUTPUT P3A_RUNTIME_SOURCE=/ABS/fixed-S-export \
PYTHONDONTWRITEBYTECODE=1 /absolute/miniconda3/bin/python -B -m unittest \
  experimental.agent_exec_adapter.test_p3b -v
```

The test module reuses only P3a fixture/sentinel utilities. Original 18 are run
separately, plus the retained independently authored repeated cancel/release /
owner reopen / fresh taskId-resource-needs boundary. New compatibility cases
retain all source commits, commands/environment/rc, runtime state/result, projected
pid and pid.log, real control/acks/envelopes, process/lease facts and audit stacks.
No fixture is auto-deleted; errors and failed roots stay intact. Independent
pidfd/saved-Popen cleanup is limited to these new cooperative no-fork fixtures;
emergency fallback is itself asserted unused on passing tests. Deletion refusal
uses only a harmless self-created decoy via original safe_delete, never a
production target. Fixed S lifecycle/prepare and applicable A control/notification/
scheduling/task-state/report regressions are separate, not a full A/old-six-repo
PASS. Historical F-A/S58/S34/F-C S41 are not closed by these green tests.

Only the experiment CLI is per-invocation and not imported by active services;
services/README content-category exemption applies. No profile version bump,
deployment, service start/stop or production status probe is appropriate here.
Independent review and dispatcher final/report consumption remain separate gates.

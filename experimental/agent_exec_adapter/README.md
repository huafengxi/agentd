# Agent execution adapters (unwired experiments)

Current **P3b** opt-in entry, mapping, controls and retained compatibility suite:
[P3B.md](P3B.md), task **a41wn0**. Current S dependency is
**4f99b25d191f61734b6b1ef89b456014413aad92**. The original P3a SHA
`e73c21579d10d9d4053c2ebbee0d4e7c6c8a9b6e` still restores its original
S `e4b79dce60f71b55899d3add5d2f127c90f1faaa` pin and narrow implementation.
No other consumer pin or recovery history is changed.

## P3a module: explicit single-host one-shot fixture adapter

Task **zmo14t**, parent **6ggidb**, approved d-09kx. This is NOT a stable API,
new daemon, replacement runner, general host, resident/P3b implementation,
model session, deployment, or production migration. Nothing outside this
experimental directory imports or launches it. No service version or start path
changes. D5 stays closed; D6 adds nothing.

## Fixed dependency and responsibility boundary

`runtime-dependency.json` pins **serviced
`4f99b25d191f61734b6b1ef89b456014413aad92`** in this current commit, including SHA256 of every runtime
Python source. Supply a clean read-only `git archive` export explicitly through
`--runtime-source`; there is no search of `~/m`, sibling production tree or
installed fallback. `load_runtime` validates those bytes and imports the package
under a private fixed-revision name; `state/adapter-imports.json` records actual
module `__file__` paths. Run with `-B`/`PYTHONDONTWRITEBYTECODE=1` to keep that
export read-only. S's legacy dependency **A0
`9625bdc15f853e49a6312d20dc429e23330e3d89`** is unchanged; the opt-in runtime
modules do not load S's legacy `serviced.py` or switch its envscrub pin.

```text
read-only A source                  read-only fixed S source
  register-json -> registration       runtime.contract/prepare
      | spec + prompt (original)          | Git/object prepare
      v                                    v
  original scheduler --once       S Runtime + Kernel ONLY
    target / needs / resources     local node flock / per-run lease FD
    slots / provider policy        Popen/setsid / wait / identity / groups
      | enable (scheduler only)    TERM->KILL / convergence / result.json
      v                                  ^              |
  opt-in Adapter -------------------------+              |
    fixed fixture mapping + agent env -> Task             |
    stop/cancel protocol -> S cancel ---------------------+
    S facts -> sole agent pid projection <----------------+
    original Runner helper methods: ask/reaper route,
      control ack, final notification (NO supervision calls)
    original task_state -> provider -> scheduler -> needs successor
```

`Runner` is imported deliberately, as a **semantic helper object**. Adapter never
calls `Runner.service/spawn/observe/kill_proc/do_stop/do_restart/do_clear` or
`handle_controls`. No legacy runner is launched in the selected workspace.
`Runner.write_pid` is reused solely by `Adapter.project` as an atomic agent
protocol writer, not as a second observer/supervisor. Original
`Runner._ask_inbox_env/resolve_reaper/notify_tick/pending_reqs/write_ack` retain
routing, receipt, report/notice policies. `task_state`/scheduler are unchanged;
there is no copied success predicate. Runtime does not import A, read reports,
agent inboxes, reaper, or provider policy.

One adapter lifetime owns S's node flock, which also fences all its projections.
An exclusive immutable **workspace binding** names the complete fixture selection,
S source, runtime root/revision; a different root/host/selection cannot evade the
fence. Runtime node/run locks remain under the separate local runtime root.
`runner.refresh_lock` publishes the **actual adapter owner PID/procStart and
startedAt/updatedAt** at `workspace/agents/run/agentd.<host>.lock` while it lives.
This synchronized heartbeat is not a flock or a fabricated payload liveness
record. The scheduler genuinely blocks before the adapter publishes it.
No legacy runner is compatible with this selected workspace: use this experiment
only in a new isolated tree, never alongside a runner/loop.

## Narrow experimental declaration and mapping

The finite external manifest is local experiment input, **not a registration
schema extension**. Exact shape:

```json
{
  "experiment": "p3a-one-shot-fixtures-v1",
  "host": "fixture",
  "tasks": {
    "task/a": {
      "command": "p3a-fixture a",
      "sources": [{"alias":"fixture", "repository":"file:///ABS/NEW-SANDBOX/source", "revision":"FULL_40HEX_COMMIT", "requires":[]}],
      "argv": ["/absolute/python", "${code.fixture}/payload.py", "/ABS/NEW-SANDBOX/tracks/a", "exit", "report", "0"],
      "env": {"SYNTHETIC_VALUE":"fixture"},
      "toolchain": {"python":"/absolute/python"}
    }
  }
}
```

The JSON above is schematic, not executable with the placeholder pin. The tests
create and commit a real file-only Git fixture, then generate valid manifests and
registered spec/prompt through the real CLI.

- Only `task/<id>`, exact host, explicit `restartPolicy=one-shot`, nonresident,
  no `profile` are supported. Unknown/unbound command, resident, auto/manual,
  arbitrary shell/model/persona invocation and unsupported manifest keys fail
  preflight. The registered command must exactly match `p3a-fixture <label>` from
  the manifest; **that string is never executed as a shell command**. Only its
  declared argv is passed to S. This is opt-in experiment syntax, not a new
  production agent command convention.
- Registration is unchanged. It publishes prompt before spec. `spec.workdir`
  must **explicitly** name S's independent work directory:
  `runtimeRoot/work/Task/<runtime_id(host,participant)>/a1/1`. It is not silently
  remapped from arbitrary legacy cwd. `workdir()` computes this before
  registration. Git source, manifest, S source, A source, runtime work/state and
  agent workspace are separate; source Git must be newly synthesized inside
  the specified sandbox, outside workspace/runtime root.
- Runtime Task id = `agent-` + first 32 hex SHA256(canonical `[host,participant]`),
  attempt=`a1`, version=1. Release request=`release-` + first 48 hex SHA256 of the
  canonical participant/spec/**actual enable**/fixture binding/prompt digest/S
  revision. S computes its declaration digest from the complete Task including
  argv/env/source pins. Adapter stores the immutable seed **and complete
  declaration** in local `state/adapter/`; spec/prompt/release/binding/route
  changes fail closed, not a new attempt. Repeated scheduler ticks, release,
  adapter ticks or same request never spawn a second copy. A new taskId via
  registration is the only agent-level rerun; no `rerun_task` is called.
- Queue-only stop is a non-executable admission with `enable=null`, followed
  immediately by **S accept(start=False) + cancel**. It is not scheduler release
  or payload permission; no source prepare/Popen happens. Otherwise absence of
  original scheduler enable means no runtime admission or pid projection.
- No ambient adapter environment is forwarded. Only declared nonidentity env
  plus original agent identity/ask routing is supplied. `envscrub.scrub_env(...,
  strip_third_party=True)` checks declaration keys before fresh explicit
  `AGENT_HOME/ROOT/SELF`, `AGENTD_TASK=1`, `AGENTD_ROOT` and original ask routing
  are injected. Parent PI_/API-key/session/resident/persona identities cannot
  leak. S exclusively creates its own HOME/TMPDIR/PATH/LC_ALL/RUND_LEASE_FD.

## Fact projection and controls

| Fixed S fact | Agent projection | Provider (original task_state only) |
|---|---|---|
| accepted/preparing/starting/running/cleanupUnknown | running, final=false; S reason preserved | pending, keeps slot; unknown is NOT success |
| succeeded, observed wait exit0 | exited/0, final=true | nonempty report: success; no report: idle |
| failed, observed nonzero | exited/actual code, final=true | failed even with report |
| failed prepare/start, no wait | exited/126, final=true; explicit S error/no pid | failed; never fabricated exit0 |
| uncanceled signal result | killed/(128+signal), final=true | failed unless original stop policy applies |
| canceled | killed/137, final=true | original stop request => canceled |
| lost, exit status unavailable, group+lease converged | stale/127, final=true | report => success (old exception), no report => failed |

Generation is 1 except canceled before any start, which projects original
`gen=0,pid=0`; S still has its own generation1 attempt result. Accepted time is
used before startedAt exists; S ISO timestamps are converted to original agent
local timestamp format. Runtime identity/request/digest/phase/reason remain
traceable in the pid document, detailed facts remain in local S run/result.
Agent `final` is absorbing: repeated projection does not refresh its timestamps
or bytes. Runtime terminal result is also absorbing.

Only original `control stop` / `cancel` requests are supported. All restart/clear
requests are rejected (including final restart/clear); final stop gets noop.
Running cancellation delegates **all** signal/identity/group/lease handling to S;
only factual terminal convergence yields an applied/noop original control ack.
`cleanupUnknown` or interrupted pending receipt is never acknowledged as stopped
or made final. Repeated ambiguous S control is not replayed; observation can
later close natural convergence. An already observed exit wins the runtime
exit/cancel race, and agent delivery remains decided by original policy.
Optional experimental expectedGeneration guards are checked against agent gen;
S control always carries explicit S generation. Old task ids/attempts cannot
address a newly registered task. There is no same-task new generation here.
No `inject` on stop, restart/clear implementation, history clearing or notify
retry extension is added.

## Explicit invocation and retained tests

From the new A source root (not a production workspace):

```sh
PY=/absolute/miniconda3/bin/python
# Export S from actual origin at the above full fixed commit into an independent
# clean directory, readonly input. All mutable roots below are NEW synthetic roots.
PYTHONDONTWRITEBYTECODE=1 "$PY" -B -m experimental.agent_exec_adapter.adapter \
  --sandbox-root /ABS/NEW-SANDBOX --workspace /ABS/NEW-SANDBOX/workspace \
  --runtime-root /ABS/NEW-SANDBOX/node --runtime-source /ABS/fixed-S-export \
  --manifest /ABS/NEW-SANDBOX/manifest.json --seconds 10
```

Default is one foreground tick; `--seconds` is bounded 0..3600, not a daemon.
Exiting the adapter does not cancel a Task: an unobserved orphan may later be
`lost`, never auto-retried. Real scheduler is separate (tests explicitly tick it).

Reproducible core suite with new absolute output, private HOME/TMPDIR/cwd/Git
config and PATH pointing at miniconda + system tools:

```sh
P3A_TEST_OUTPUT=/ABS/NEW-OUTPUT P3A_RUNTIME_SOURCE=/ABS/fixed-S-export \
PYTHONDONTWRITEBYTECODE=1 "$PY" -B -m unittest \
  experimental.agent_exec_adapter.test_adapter -v
```

18 retained tests cover real register-json/spec/prompt, initial target block,
real scheduler enable, runtime process/results, sole pid projection, payload's
explicitly labelled **fixture** report, original provider, needs successor;
exit0/no report, nonzero ±report, prepare failure/no execution, queued cancel
before/after release, running TERM→KILL + ack/slot convergence, duplicates and
final restart/clear, real owner SIGKILL/orphan/lost ±report, conservative identity,
group and lease unknown/mismatch, node lock/workspace fencing, unsupported
mapping/release conflict, stale controls, harmless deletion decoy, fixed
imports and a real resource-only block with free concurrency. No tests handwrite selected enable, final or control ack. Legacy
execution methods are replaced by throwing sentinels (also in crash-owner CLI).
Per-case command/rc/stdout/stderr, projection callsite events, run/result and
protocol originals are retained. Harness owns only its saved owner Popen;
cooperative orphan exits naturally at a barrier and is adopted/reaped by harness,
never converted into an S wait result. Payload FD inode/device and slot release
are asserted, not just field presence. No fixture tree is auto-deleted.

## Unverified / excluded and interface gates

Only cooperating foreground shell/Python-style fixtures on Linux are supported.
S requires the payload and descendants to retain its inherited lease FD; no
malicious same-UID/FD-closing/daemonizing/setsid-escaping loads are supported.
This is not a security sandbox or arbitrary crash/storage-window guarantee.
No automatic Task retries, cross-host takeover or transparent session recovery.

P/C exports are **not needed or used** for this first Python payload piece.
The original ask route is injected and validated, and existing A communication
contracts remain unchanged, but a real pi wrapper/model/TS receiver session,
ask_dispatcher blocking/ref/via roundtrip through that session, prompt handshake,
settle/model-error gates, personality/extensions and interactive report/ref
workflow are **not exercised here**. No strong inbox ack is manufactured; only
original terminal inform and control ack are part of this chain. To select such
a command requires a separately approved bounded mapping/fixture that proves
its executable, cwd, dependencies and S lease-FD cooperation. It must NOT be
silently admitted by this adapter. Resident/auto, restart/clear/inject, expanded
notify/reaper controls remain P3b/other gates, not implicit followups.

Original A scheduling/provider/task_contract/runner-final/report regressions and
fixed S lifecycle/prepare regressions must be reported separately from this
suite. Their legacy synthetic supervision/static fixtures are not selected P3a
loads, not a substitute for this actual register→scheduler→runtime chain.
Historical F-A/S58/S34 unknown/failed evidence, F-C exclusions and production
remain unchanged. Independent review and queue/dispatcher acceptance are still
required; source push or a test token is not independent PASS.

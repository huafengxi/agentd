# Local one-shot file backend — ibs7lv (unwired opt-in)

Only nonresident, explicit `restartPolicy=one-shot`, task-family cooperative
foreground Python fixtures. No model, deployment, ordinary W, resident/auto,
restart/clear, remote transport, synchronization or new service. D5 closed/D6 no
addition. Original adapter.py/p3b.py and runtime-dependency.json remain intact.

## Ownership / fixed source

```
agentctl register-json -> original registration/spec/prompt
original Scheduler.tick -> enable (sole release authority)
    independent OS A FileAdapter (local A flock + workspace binding)
       -> fixed S file client send / exact receipt / last-observed snapshot
    independent OS S file Owner -> original Runtime / Kernel / node.lock
       -> prepare / Popen / wait / identity / groups / stop / result
A project -> sole selected agent pid writer
payload -> explicitly synthetic report
original task_state/provider -> original scheduler resources/needs successor
original Runner helpers -> control ack / notify_tick / reaper policy
```

A imports fixed S contract/prepare/protocol only, does not construct Runtime or
Kernel, hold S's lock, spawn/poll/wait/kill payloads or run Runner supervision.
The S source files are verified against `file-backend-dependency.json`, including
Owner/Kernel sources. The client path is `runtime/file_owner/protocol.py`;
actual client imports and hashes are recorded in workspace/run/file-adapter.
Owner runs explicitly from that same fixed clean export, separately. The S->A0
pin is not changed. No ambient source fallback or pyc import permitted. Use a
separate process if a different runtime package has already been loaded.

A uses original local Runner lock inode `workspace/run/locks/agentd.HOST.lock`
with nonblocking flock; this also excludes a legacy writer on that workspace.
Immutable workspace binding fixes host/selection/full manifest/S source/revision/
protocol/runtime root, preventing another root from bypassing the fence. These
are local A projection locks, not another execution kernel or cross-machine lock.
A only refreshes original synced heartbeat with its own actual process identity;
that heartbeat is NOT S availability. A exit releases only A lock. No automatic
owner launch/recovery/stop. Restart A with identical binding; existing payload
remains waited by S. S restart retains its own orphan/null/unknown gates.

## Inputs and mappings

Same finite manifest/task bindings as README.md, but
`experiment=agent-file-one-shot-fixtures-v1`. Original strict registration fields
and explicit host/workdir retained. Literal `p3a-fixture NAME` binding is not
executed as a shell command. Sources must be synthetic file:// Git inside the
explicit sandbox, disjoint from workspace/protocol/runtime. Source export and A
source are separate. Manifest outside all mutable roots. Environment uses the
original scrub/ask routing helpers; only declared env and explicit agent identity
enter S declaration, never inherited process env. S owns HOME/TMPDIR and native process-group identity.

Runtime id=`agent-`+SHA256(canonical [host,participant])[:32], a1/gen1/version1.
Declaration requestId=`release-`+immutable seed fingerprint[:48]; seed fixes real
spec, prompt digest, original enable (or null queued-stop), binding and S revision.
S canonical/digest functions compute declarationDigest and the outer body digest
`{operation:file-owner,envelope:...}`; send/receipt use identical scope.

| A intent | S action / stable external requestId |
|---|---|
| enabled Task or queued stop | accept (never start at admission) |
| actual enable + no stop intent | start |
| exact original stop request | cancel with S expectedGeneration=1 |
| observation | read snapshot only; never unknown status allocation |

Each external rid=`af-`+fingerprint(participant,key,full envelope without rid)[:48].
Keys accept/start/cancel:AGENT_REQ_ID are persisted **before** publishing. After
crash, a published request is only read; missing publication is unknown and NOT
replayed. Same rid/body returns original receipt, conflicting bytes refused.
Receipt must match exact envelope/body digest and done result. Pending/timeout/
rejected/blocked/damaged means local diagnostic, no new rid, no retry/release/
cancel replay. Exact done means **operation returned**, NOT workload finished.
A local journal is protocol identity/binding only, not a second run result log,
execution queue, compensation worker or automatic retry system.

Queued stop maps agent gen0 to S gen1. It may accept without enable solely to
cancel; pending stop is a persistent barrier even if scheduler later enables or
original req disappears. Never project accepted as actual running. Original
scheduler already reserves enabled/no-pid tasks. Running stop's exact terminal
receipt must agree with immutable result (native identity/group convergence) before ack.
An exact done cleanupUnknown remains unacked even if later observation changes:
this deliberately conservative alpha has no invented terminal control receipt.
No second cancel request is allocated to get around it. Terminal noop receipt
also must bind the exact run. Stale expected agent gen/timestamp -> noop;
unsupported actions -> rejected; final stop -> noop. Final absorbing, redo by
new agent taskId only, no runtime Task retries.

## Facts, success and availability

Snapshot API/node/semantics and run id/kind/node/declaration/attempt/gen/digest/rid
must match. Terminal projection requires byte-equivalent decoded immutable
`result.json` (read-only original publication), not just snapshot phase. A never
calls directory-creating Runtime readers. ownerEpoch is not generation; top-level
snapshot observedAt/mtime is not payload observation. pid.lastAliveAt derives
from the original run observation.observedAt (or startedAt before observation).
An identical/older run never refreshes pid or makes it appear newly observed.
No new nonterminal fact for a local interval produces an availability-unknown
diagnostic, NOT an owner-dead claim or payload exit. Existing final is untouched.

| S fact | Agent / original task_state policy |
|---|---|
| accepted/preparing without started identity | no fabricated running pid |
| running/cleanupUnknown with identity | nonfinal running reservation, last-observed only |
| succeeded / real wait0 | exited0; report=>success, no report=>idle |
| failed real nonzero | exited actual code; failed even with report |
| prepare/start failure without wait | exited126, explicit S error, no payload |
| canceled | killed137; original stop/report classification retained |
| other signal | killed128+signal |
| lost, no wait/null status | stale127; report=>success, no report=>failed |

No new success rule. 125 is original canceled contract code, not a synthesized S
wait; canceled run retains original P3a killed137 mapping. notify task_done /
task_failed / task_canceled are signals, not provider/review PASS. Original
notify_tick handles none, legal recipient and fallback note; no TS receiver,
model, strong claimedTs injection, direct reaper bridge or extra wait files.

## Invocation / testing

Explicitly launch independent fixed S owner first using its PROTOCOL.md. Then:

```
python -B -m experimental.agent_exec_adapter.file_backend \
  --sandbox-root ABS_NEW --workspace ABS_NEW/workspace \
  --runtime-root ABS_NEW/node --protocol-root ABS_NEW/protocol \
  --runtime-source ABS_FIXED_S --manifest ABS_NEW/manifest.json --seconds 30
```

Bounded foreground opt-in, default one tick, no production import. Test with
`AF_TEST_OUTPUT=ABS_NEW AF_RUNTIME_SOURCE=ABS_FIXED_S python -B -m unittest
experimental.agent_exec_adapter.test_file_backend -v`, in explicit isolated
HOME/TMPDIR/cwd/PYTHONPATH. Retain all roots/logs. Harness is separate from A,
may register/tick scheduler/build Git, starts its own A/owner Popen and closes
only those saved handles/strong fixture identities. Payload audits require S
Kernel.launch, git prepare requires original S Git.run; no test-module blanket
whitelist. Harness adoption/reap is never passed to runtime as wait evidence.

Cooperative Linux-only sandbox, not hostile UID/PID race protection, exactly-once
storage or arbitrary crash-window guarantee. Pending/unknown needs manual
inspection; no GC/recovery service. Legacy synchronous P3a18/P3b25 and fixed S
file17/lifecycle43/prepare28 are separate regressions; warnings/internal skips
and historical F-A/S34/S58/F-C S41 unknown remain separately reported. Independent
review and dispatcher real final/report acceptance are separate gates.

# agentd source recovery — task rd6-a-171006

**Historical accepted-baseline record below.** The reviewed task-tab incremental
projection and its separate standalone adaptation are documented in
[delta-README.md](delta-README.md), with delta-map/net-paths/test-results JSON.
verify.py checks both layers without rewriting this historical evidence. The
original Bash3.2 gap described below was subsequently supplemented by independent
baseline review; this increment does not change helper/shell code.

Candidate source only. No production references, profiles, service activation,
installer, or caller preconfiguration are supplied here. Original main remains
an ancestor; original remote identities remain reachable through preserve refs.

## Provenance

- Original agentd main: `50b8e7fc84c4ea287cdab4206219b337eb3e8fb8`.
- Unified import baseline: `990253da3b5c5ec83fd43a2a463f39ed6d42aadf`.
  Its agentd subtree equals the original main, including every file mode/blob.
- Initial source freeze: `fb758234da0bd717d511dd41999679fa0bbf4427`.
- Closing committed source: `4ac4631fbdbf27f3c400805b233dc1d7a9892d46`.
  The intervening commit belongs only to pi-wrap thinking defaults and is a
  recorded no-op here. Its earlier uncommitted files were not exported.
- Projection tip: `383c1aa2875e2283a38b5eed6bbc69350710fa9b`.
- Standalone adaptation: `06735ba3abd37f5204bd118fe57dec044cdf46ba`.

`source-refs.json` records all archive/retired refs, original OIDs, preserved
mapping and complete per-ref object closures, not just main. Original symbolic
ref relationships (where present) remain part of the historical preservation
handoff; no preserve ref is changed by this recovery.

`source-map.json` lists all **13 first-parent source commits**, in order, with
10 owned-path projection commits and three no-ops. Each projected path has its
source/target name, before/after blob and mode, and deletion flag. Projected
commit messages carry the full source SHA, paths and taskId. This is not a
cross-module cherry-pick or history rewrite. The source commits themselves are
preserved in the prior complete provenance bundle, not spliced into agentd main.

The unified root installer and portable installer/example were explicitly
excluded. Their creation and later removal are recorded under `excluded`,
including original blobs/modes. Neither their transient trees nor install.py
are recreated. The six effective portable files are owned by this repository.

`adaptations.json` explains every one of the 19 differing adapted paths;
`net-paths.json` checks all 37 non-recovery files, including unchanged inherited
files, against original, frozen source, projected and delivered blobs/modes.
It does not infer completeness from a path count or final subtree alone.

## Interface adaptations (no scheduling-policy change)

- `platform_paths.py` is a same-repo thin helper, derived from source commit
  `9809211b1e91adabb0dfda570bd4a79dc4313d21`. Local contract v1 is:
  explicit CLI root first; otherwise nonempty PLATFORM_WORKSPACE expanded and
  made absolute; otherwise the real source repository's parent. No marker,
  sibling import, MODULES manifest or unified-repo fallback. Fixed-release
  callers must inject the state root. Fixtures cover whitespace, cwd,
  symlinks, relative overrides, empty overrides and tilde expansion.
- Python entrypoints import siblings through their real directory. Both Bash
  supervisors resolve real/relative/symlink entries without GNU readlink -f;
  scheduler concurrency argv assembly remains the projected Bash-3.2 fix.
- proto/task_state/envscrub stay agentd-owned and versioned together. Cross-repo
  consumers must explicitly locate/pin this repository; no consumer activation
  is part of this commit. proto's adaptation is a doc-only path cleanup.
- Registration, resident lifecycle, snapshots, report transport and widgets
  retain their projected mechanisms. All seven read-only widget sections use
  the host-provided `src` to locate the adjacent report.py. Caller-specific GC
  commands/policies are not copied into an independent mechanism repository.
  Root unified README boundary/portable usage text is incorporated in the local
  README; six-module deployment/integration instructions stay caller-owned.
- Portable uses root-level files and archive prefix `agentd-minimal/`.
  Its **independent narrow root semantics remain unchanged**: default package
  workspace; relative override anchored at package source; no ~/$ expansion.
  env -i and Python -I -S -B remain mandatory. No download, installation or
  model call is added. Test fixtures include forbidden downloader/install
  command traps, read-only doctor, real shell final/report, ctl, relocation,
  bytecode absence and literal-root/symlink checks.
- e2e defaults to isolated standalone core; cross-repo GC/TS/wrap checks require
  explicit --integration dependencies, and S53 additionally requires --live-pi.
  The offline matrix never calls it. Fixture helper copies use local layout;
  cleanup routes through safe_delete under a newly created authorization root.
  This changes test packaging, not runner/scheduler semantics.

## Reproduce

Use your own Python >= 3.10 (stdlib) and POSIX tools/bash; no install step:

```sh
python3 recovery/run-tests.py
python3 -I -S -B portable/test_portable.py
python3 recovery/verify.py --source /path/to/platform-provenance.git --origin /path/to/agentd.git
python3 recovery/make-shell-fixture.py > /new/path/bash-fixture.sh
/bin/bash /new/path/bash-fixture.sh
```

run-tests.py creates fresh isolated HOME/TMPDIR and retains logs/results.json.
The shell fixture is generated from actual helper/supervisor snippets, uses a
fresh retained /tmp root and never starts a supervisor. It can be shipped as
one standalone file to a Bash 3.2 machine. verify.py is read-only: it checks
per-source projection, all before/after blobs/modes/deletions, no-ops, installer
exclusions, exact adaptation scope, net files, original ancestry/refs/objects,
and (if supplied) the actual pushed origin main.

## Validation result and boundaries

Linux conda-Python run: all nine root test scripts, standalone e2e (50 checks),
and explicit portable -I -S -B (10 tests) passed. e2e excludes seven cross-repo
scenarios and reports three unavailable caller-context subassertions explicitly;
these are not counted as coverage. Real and symlink Python/shell entrypoints,
spaces and root precedence passed. Shell fixture also passed on local Bash 4.4.20.

**Actual Bash 3.2 execution is not yet verified**: mac appeared in an earlier
rsh list, but fixture submission returned exit 125 (worker not registered),
and subsequent lists omitted mac. This establishes channel unavailability,
not that the physical host is offline. The dispatcher resource reply confirmed
no currently verified alternate Bash 3.2 path; it did not waive the criterion.
No download/install, service restart or channel repair was attempted.
Bash-3.2-compatible syntax and positional-argument assembly are retained, but
local Bash 4.4.20 success is not claimed as a real Bash 3.2 pass. No model, external
pi-wrap/TS integration or production/multi-host deployment validation is claimed.
Follow-up independent review must assess this precise gap before accepting the
full requested matrix. All source passing review still does not open a
production activation gate.

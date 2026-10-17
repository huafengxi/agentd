# F-A B1 second-round test correction — task/i4ohu5

Baseline: `7c6b031db1d160564b985d8d89e433687a6672ee`.
Test correction: `1cffece0cdf9d04b8edcf03b4e3fc2e30886a74e`.

Source is the independent review task/u5aq6g B1, not another platform source
projection. The original full core run returned 1 with S58 `KeyError: reason`;
its selected request and cancel receipt were not retained. Six subsequent
traces were normal. Neither those successes nor our fixture establishes the
original event's cause. The original review evidence remains unchanged.

`b1-evidence.json` records a **constructed** same-clock/reverse-suffix example:
real CLI control writes a stop without reason, then real CLI cancel writes a
stop with reason; both return their own request IDs. Filename ordering selects
the control request. The old S58 actually raises `KeyError` in this fixture.
The correction opens the file named by the cancel stdout receipt. It keeps
all action/reason/from/id assertions and the liveness gate; no product code or
protocol changes, sleeps, retries, req.get fallbacks or assertion removal.

Run `python -B test_cancel_receipt.py` in a fresh synthetic HOME/TMPDIR and
workspace. This exercises the actual S58 with controlled CLI ID inputs and
also real cancel explicit/blank/default reasons, final/no-spec rejections,
and control's absent/blank reason and final bypass. Fixtures are retained;
S58's inherited cleanup uses its own newly-created safe-delete base. Optional
caller constants require the same explicit fixed caller input as the core
suite; without it the existing S58 platform subskip is reported, not hidden.

Historical recovery files, source mappings, old refs, failed runs and accepted
pins are unchanged. In particular `fresh_verify.py`'s HEAD check intentionally
still describes its historical F-A runtime tip; it is **not** a verifier for
this later test-only descendant. Do not run it against the new HEAD and claim
success. The new task's `audit.py` / `evidence/audit.json` independently check
the new delta, old mapping blobs, refs and object closure. Final fixed-pin
clean-export suite results and complete argv/cwd/env/rc/logs are delivered in
task/i4ohu5/report.md and its evidence directory, not rewritten into old
recovery outcomes. This is not deployment, F-P/F-C/P3, or an independent PASS.

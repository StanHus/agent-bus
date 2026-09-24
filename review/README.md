# agent-bus focused code review checks

Reviewed source: StanHus/agent-bus, commit a4a7bbc.
Scope: agent_base.py lines 433-508, plus the called bus and lifecycle helpers.

The supplied patch fixes ONLY the plan directive argument-binding bug. It is
not a full reliability patch and does not fix replay, restart, validation,
security, heartbeat, audit, or circuit-breaker problems.

## Run against a checkout

Requires Python 3.10+ and its standard library only. From the directory containing
this test script:

```sh
python agent_bus_regression_tests.py --repo /path/to/agent-bus
```

Use a checkout of a4a7bbc to reproduce the reviewed behavior. The script reads
agent_base.py and comms.py from that directory. The AST loader executes selected
original functions with injected dependencies; it does not import the production
modules, call BaseAgent.__init__, invoke private skills, contact external
services, perform real process exits, or write outside temporary test directories.

These checks assert desired reliability contracts. The original implementation
is expected to FAIL the targeted robustness checks. They are not all independent
bugs and their ratio is not a repository quality or test-coverage score. The
malformed-message preservation and replay tests deliberately specify stronger
contracts than the current implementation provides.

## What was executed during this review

Environment: Python 3.13.5, temporary filesystem, mocked external integrations.
Execution source: locally transcribed excerpts of the functions retrieved through
GitHub at the pinned revision. Executable paths under test were retained; comments
were simplified. This was NOT an execution of a byte-verified full checkout or
of the macOS/iCloud deployment. Source excerpts are not bundled here; run this
script against your checkout for independent verification.

Selected methods: tick, run, _circuit_breaker_open, _trip_breaker, healing_handler.
Selected bus functions: now_iso, audit, inbox_dir, outbox_dir, drain_inbox, respond.
Other agent hooks, tracing, alerts, and heartbeat were mocked.

Results: 21 checks; 4 baseline checks passed; 11 assertions failed and 6 checks
raised unexpected exceptions. See test_results.txt for all failures. The single
plan-dispatch check passed after applying the named-argument change to the
isolated excerpt; see plan_fix_test.txt.

The tests are designed for this function layout. Adapt the isolated loader as
new helper methods are introduced during a refactor; do not treat these checks
as a substitute for full integration tests, fault injection, deployment tests,
or tests of handler-side idempotency and external side effects.

## Reviewed GitHub blob identifiers

agent_base.py: 93fe264ebeed07420e4fb460b9e3c93aecab9df0
comms.py: 8983f48f803b2903ccb96d533097dfc7777a6ed0

These identify the upstream files returned by GitHub, not hashes of the local
excerpts used for isolated execution.

## Patch and retest

After reviewing the patch, apply it from the root of your checkout:

```sh
git apply --check /path/to/plan_argument_fix.patch
git apply /path/to/plan_argument_fix.patch
python /path/to/agent_bus_regression_tests.py --repo . \
  AgentBusContractTests.test_plan_dispatch_preserves_named_arguments
```

The patch has not been applied to the remote repository. Remaining fixes require
an explicit message-lifecycle, recovery, authorization, and shutdown contract.

## Loader adaptation for the reliability revision

The revision that answers this review keeps every original extraction point (BaseAgent.tick, run,
_circuit_breaker_open, _trip_breaker, healing_handler; comms.now_iso, audit, inbox_dir, outbox_dir,
drain_inbox, respond) and adds helpers those functions call. Per the note above, the loader's name
sets were extended and nothing else in the assertions was changed:

* comms set (line 73): `validate_identifier`, `read_message`, `quarantine_message`; round 1 of the
  adversarial fixes added `validate_agent_name`, `agent_subdir`, `known_agents`.
* BaseAgent set (line 82): `_audit_safe`, `_write_health`, `_dispatch`, `_validate_result`,
  `_load_outcome`, `_save_outcome`, `_process_message`, `_controlled_exit`, `_reconcile_restart_intent`;
  round 1 added `_safe_str`, `_count_failure`, `_quarantined_count`; round 2 added `_operator_key`
  and `_claim_is_live` (and `signed_message` to the comms set).
* `test_baseline_open_breaker_heartbeats_without_work` seeds `_circuit_breaker_pause_until` with
  `time.monotonic() + 1000` because the cooldown now uses the monotonic clock as section 3 asks. The
  original `time.time()` seed also passes, but only because epoch seconds exceed any monotonic reading.

Result on the revised checkout (Python 3.14.6): 21 checks, 21 pass.

# External review of agent-bus @ a4a7bbc (received 2026-09-24)

## Verdict

It has a sensible, readable outline, but it is not production-quality reliability code as written. There are
concrete correctness bugs, not merely style issues, and failure paths that can repeat completed work, prevent
later messages from being processed, or make the circuit breaker ineffective. Not approvable unchanged for agents
performing consequential, non-idempotent actions.

Reviewed: the pinned commit a4a7bbc, tick(), its lifecycle helpers, and comms.py. A proper fix requires changes
beyond those 76 lines: message acknowledgement, retries, restart handling, and handler-side effects need a coherent
contract. "Works" must become a set of testable guarantees: each failure produces a defined, recoverable outcome.

## What is good

The lifecycle is easy to follow: process messages, periodic work, scheduled maintenance, heartbeat. Shared handling
avoids every subclass implementing its own bus protocol. Heartbeating while the breaker is open distinguishes an
unavailable worker from a dead process. Temp-file-and-rename is a sound foundation for publishing complete files.
Preserve that simplicity. The problem is not the absence of a broker nor the if/elif dispatch; the failure semantics
are incomplete.

## 1. Definite bug: the `plan` directive passes arguments incorrectly (line 463)

`p = self.plan(title, steps, rationale)` but plan()'s parameters begin `title, requirements, steps`; rationale is later.
Steps become requirements, rationale becomes steps, rationale stays default. Fix: keyword arguments
`self.plan(title=title, steps=steps, rationale=rationale)`. Longer term: make optional plan() params keyword-only after
auditing callers; validate `steps` is a list of strings and `rationale` a string.

## 2. The exception boundary protects only part of message processing (lines 438-493, esp. 472-477)

Only the subclass handle(req) call is guarded. Unguarded: extracting request fields, built-in handlers,
expanding skill arguments, publishing the response, interpreting the result.

| Input or failure | Current consequence |
|---|---|
| Inbox file containing valid JSON `[]` | `req.get(...)` raises before dispatch |
| `dream_sequence()` raises | The tick aborts rather than a controlled per-message failure |
| `invoke_skill` request with `"args": null` | Argument expansion raises before entering invoke_skill() |
| Subclass returns `None` | respond() fails unpacking the payload |
| Subclass returns `{"ok": "false"}` | Non-empty string treated as success |

The first is worst: a valid-JSON-but-invalid-schema message stays in the inbox and raises again every tick, blocking
later messages. The try inside drain_inbox() does not protect the consumer after the generator yields.

Improvement: explicit boundaries for decoding, validation, dispatch, result validation, completion. Validate every
request before dispatch and every result before publishing. Same exception policy for built-in and subclass handlers.
Malformed messages need an explicit rejection or quarantine policy; distinguish malformed message from transient
filesystem read failure (the latter must not be classified as bad input and deleted). Do NOT solve this with one broad
`except Exception` that deletes the message regardless: that turns visible failures into silent data loss.

## 3. The circuit breaker does not stop the current batch (lines 434-436, 479-485)

Checked only at the start of tick(). _trip_breaker() inside the loop sets a future pause time but does not interrupt;
remaining messages and idle_cycle() still run. Test: eight failing requests, threshold five: all eight handlers ran.
Every falsey `ok` counts as an infrastructure failure although an invalid request or business rejection need not
mean the agent is unhealthy; a successful unrelated request resets the shared counter. While open, the breaker blocks
all inbox handling, including a healing directive meant to restore service.

Improvement: a small breaker state machine (closed, open, limited-probe recovery). Stop accepting further affected
work once it opens; finish recording and acknowledging the current message before stopping the batch (an immediate
break mid-completion is another replay opportunity). Separate request rejection from dependency failure; decide which
operations share a breaker; decide whether authorized recovery commands stay available while ordinary work is paused.
Use time.monotonic() for the cooldown, not time.time().

## 4. Completed work can execute again when replying or acknowledging fails

Order is: perform work -> write response -> delete request. No durable completion record or dedup. If the work
succeeds but the response write fails, the request stays eligible and runs again. If deletion fails the exception is
swallowed and the request stays eligible. A crash between stages creates the same ambiguity. Both non-crash cases
reproduced: the handler ran twice across two ticks.

Improvement: separate execution retry from response-delivery retry. Persistent lifecycle:
`received -> claimed -> executing -> outcome recorded -> response pending -> acknowledged`. Record a stable request
identity, input fingerprint, execution status, result. A duplicate of a completed request returns the recorded result
instead of executing again. Reusing an identity with different input is rejected. A completion cache alone is not
exactly-once: there is still a crash window between an external side effect and recording it; consequential
operations need downstream idempotency keys, an atomic mutation+record where possible, or reconciliation.
Define ownership: one consumer per agent, or an atomic claim/lease with recovery. drain_inbox() reads without claiming.

## 5. `heal: respawn` can create a repeated-restart loop

The respawn handler calls os._exit(0) before returning to tick(); the response and request deletion never happen.
Under supervisor restart the daemon meets the same respawn request and exits again. `finally` does not help:
os._exit() terminates without cleanup. Improvement: treat restart as a deferred lifecycle action. Persist the accepted
restart intent and message outcome, complete acknowledgement, then request controlled shutdown. On startup, reconcile
any outstanding restart intent without blindly executing it again. Replacing _exit with sys.exit() does not fix the
ordering. Also: soft_reset deletes all matching temporary files in inbox and outbox without knowing their writers are
inactive; a recovery routine must not delete an active writer's work because of a temp suffix. Use ownership or
defined stale-file rules.

## 6. Heartbeat and error reporting are not independent of failures

Heartbeat happens after processing and maintenance; an escaping exception skips it; a blocking handler delays it
indefinitely. audit() does filesystem I/O that can raise; run()'s exception handler calls audit() without another
boundary, so an audit storage failure terminates the run loop while reporting a different failure. Both reproduced.

Improvement: heartbeat attempt in a top-level finally; error reporting resilient to its primary destination failing;
retain exception traces and correlation ids, not only truncated strings. An attempted heartbeat is not a guaranteed
write (the helper suppresses exceptions). finally does not solve a handler that never returns: require dependency
deadlines and an execution strategy that enforces them. Monitoring must distinguish alive, making progress, breaker
open, and current operation overdue. Decide whether audit is best-effort telemetry or mandatory evidence; for
mandatory audit the right failure policy may be to pause consequential work.

## 7. Transport fields need validation and a defined trust boundary

`caller` and `event_id` come from the request and are used to build filesystem paths unvalidated. Traversal components
can write response files outside the agent root or outbox (severity depends on who can submit messages). Also a
correlation bug: respond() merges the transport event_id with the handler payload in an order that lets a payload
`event_id` overwrite the authoritative one; reproduced a response stored under one request's filename containing a
different event id.

Improvement: treat agent names and event ids as bounded identifiers, not paths. Reject separators and traversal,
validate destination against a registry, keep reserved transport fields under transport control, define the
relationship between envelope id and filename. Privileged operations (restart, skill invocation) need authorization
from a trusted identity mechanism, not a self-reported caller string.

## 8. Maintainability should follow the reliability boundaries

tick() combines dispatch, tracing, breaker policy, delivery, acknowledgement, maintenance, liveness. Extract components
around real contracts: request validation, dispatch, execution outcome recording, delivery/acknowledgement, health
policy. Inject transport, clock, telemetry so failures are testable without patching module globals.

Also: bound work per tick (message or elapsed-time budget plus per-operation deadlines); _maybe_dream() uses
`replace(minute=minute + 5)` which fails near the end of an hour (use timedelta, define missed-run behavior, persist
completed slots); verify claims at the layer that enforces them (the published bd.py is a stub).

## What was tested

21 isolated contract checks (agent_bus_regression_tests.py, in this directory) against transcribed excerpts. 4 baseline
checks passed; 17 targeted robustness expectations failed or errored. Run: `python3 agent_bus_regression_tests.py --repo ..`
The included plan_argument_fix.patch fixes only the argument binding.

## Required before approval

1. Fix the deterministic control-flow defects: plan argument binding, request/result validation, built-in handler
   containment, breaker batch stopping, restart ordering, transport-field protection.
2. Specify and implement the message lifecycle: ownership, durable outcomes, dedup, delivery retries, uncertain external
   outcomes, quarantine, recovery. Decide semantics before adding retries.
3. Test failures at every lifecycle boundary: before execution, after a side effect, before and after recording the
   outcome, during response publication, during acknowledgement, during restart. Add disk-full, permission, logging,
   blocking-handler, duplicate-request, and overlapping-consumer tests.

Bottom line: keep the straightforward lifecycle and shared infrastructure. Fix the obvious bugs, but put most effort
into explicit message ownership, durable outcomes, safe retries, controlled shutdown. More try/except, or a rewrite
with async, would not on its own make this dependable.

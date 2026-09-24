# agent-bus

Two files that ran a 56-daemon agent fabric for two months on one Mac mini, with no broker,
no framework, and no dependencies beyond the Python standard library. This is the reliability
revision made after an external review of commit a4a7bbc (`review/REVIEW.md`): the transport and
the lifecycle are the same, and every failure now has one defined, tested outcome. The guarantees
are listed in `CONTRACT.md`.

| File | Lines that matter | What it is |
|---|---|---|
| `comms.py` | `validate_identifier`, `drain_inbox`, `respond`, `send_directive`, `audit` | The bus. A message is a JSON file delivered by atomic rename. Identifiers are validated before they touch a path; bad input is quarantined with its bytes intact; audit never raises. |
| `agent_base.py` | `BaseAgent.tick()`, `_process_message()`, `_dispatch()` | The loop every agent ran. Inbox, four universal directives, durable per-message outcome record, circuit breaker state machine, deferred restart, audit, idle work, dream, heartbeat. |
| `bd.py` | stub | No-op functions standing in for the private bead ledger. The import is guarded: `agent_base.py` also runs with no `bd` module and reports which case it is in. |
| `CONTRACT.md` | | The message lifecycle, each guarantee, the failure it covers and the test that pins it. |
| `tests/test_contracts.py` | 60 tests | Runs the real modules against a temporary root. |
| `tests/adversarial_*.py` | 104 tests | Adversarial suites from three rounds (replay, trust, breaker-clock, liveness lenses); every case names the guarantee it attacks. |
| `review/` | 21 checks | The external review and its regression suite. |

## What it carried

| | |
|---|---|
| Long-running daemons | 56 |
| Agents heartbeating at once | 47 |
| Messages surfaced to the operator, May 27 to Sep 23 2026 | 16,083 |
| Shared audit files | 190 |
| Bus latency in the selection shootout (2026-06-02) | p95 0.402 ms, p99 0.508 ms |
| Crash survival in that shootout | 50 / 50 |
| Fine-tuned model versions driven end to end | v92 to v110 |

The work it drove: fine-tuning question generators on Fireworks, judging them against an external
evaluator, chaining the next run from the result, and refusing to sit idle. The daemons that did that
(chain conductor, continuity watchdog, token manager, mesh doctor) are private; the two files here are
what they all had in common.

## Why files

Every alternative brought a process to run, a port to bind, credentials to manage, and a new single
point of failure. `rename()` is atomic on POSIX, a crashed writer leaves only an orphaned `.tmp`, and a
directory tree in iCloud made two machines share one bus for free. The shootout was run before the
choice, and its numbers sit in the module docstring.

## What the review changed

The reviewer found the outline sound and the failure semantics incomplete: a valid-JSON list in the
inbox blocked every later message, a failed reply re-ran completed work, the breaker did not stop the
batch, `heal: respawn` could loop under a supervisor, `caller` and `event_id` reached the filesystem
unvalidated, and an audit write failure could kill the run loop. The revision answers each with a
guarantee an operator can check on disk:

* `state/outcomes/<eid>.json` walks `executing -> recorded -> delivered -> acknowledged`; reply and
  acknowledgement are retried from the record, never by re-running the handler. Duplicates return the
  recorded result; the same id with different input is rejected.
* Malformed or schema-invalid requests go to `quarantine/` byte-for-byte with a reason sidecar. A
  transient read error leaves the file alone.
* Breaker: `closed -> open -> half_open`, monotonic clock, one probe, doubled cooldown; the batch stops
  at the threshold after the current message is acknowledged; `heal` still gets through.
* Respawn is acknowledged before the exit and reconciled at boot. Heartbeat sits in a `finally`.
  Audit never raises. `state/health.json` separates alive, progress, breaker and overdue.

A second, adversarial round then attacked the guarantees from four lenses and each finding became a
test and a fix: the `executing` claim is an exclusive create (two consumers cannot both run one
message); a duplicate arriving after the acknowledgement record was lost is replayed; one caller's
unwritable outbox no longer stops the batch or trips the breaker; the exit for `heal: respawn` waits
for that request's own acknowledgement; only regular files are opened from the inbox (a symlink or
FIFO is quarantined, size is checked before the read), replies are written with exclusive-create so a
planted link is never followed, agent names may not start with `_` and callers are checked against
the registry; the operator HMAC covers `invoke_skill` arguments; the breaker has no open -> open edge,
reopens on a storage failure during the probe and pauses idle work while half open; dream windows
survive midnight; audit and the exception boundary tolerate exceptions whose `__str__` raises and
results json cannot encode; boot survives a malformed restart intent, a read-only `state/` and a
`current_op.json` left by a hard crash.

A second adversarial round (same four lenses) closed the gaps the first left: the `executing` claim
carries its owner's pid and time, so a consumer that meets a live claim backs off instead of calling
it a crash, while a corrupt or attempt-less record can no longer wedge an idempotent request; records
that are not objects with a known status read as unreadable; requests are fingerprinted as the handler
sees them (transport defaults included); the breaker sees the handler's class in the tick it ran even
when the reply cannot be delivered, redelivery retries are not charged to the message budget and a
caller whose outbox failed waits a tick, so one broken caller can neither trip nor block the breaker
nor starve others; the dream attempt is written before the dream runs (a crashing dream is bounded to
three runs), a failed slot is never rewritten as missed and a wrong-shaped slot file is repaired; the
operator HMAC is a canonical JSON message that includes the addressed agent (a signed heal cannot be
replayed to another daemon), an empty or unreadable key refuses every privileged request and health
says so, a malformed `auth` and unencodable `plan` text are rejections; `send_directive` creates its
temp file exclusively; a request nested too deep to decode or named at NAME_MAX is quarantined once;
an inbox that cannot be listed is audited and reported as unknown, not empty; boot survives a lock
file it cannot open and a volume without `flock`; a rejected duplicate of the respawn id cannot
trigger the exit; a result value whose `__str__` raises is answered in the same tick.

**This is at-most-once per event id, not exactly-once.** A crash between a handler's side effect and
the outcome record is answered as `uncertain` and not re-run; handlers with consequences must carry
`event_id` downstream as an idempotency key or opt into `IDEMPOTENT_KINDS`. Details in `CONTRACT.md`.

## Run

```sh
python3 -c "import comms; print(comms.new_event_id())"
python3 -m unittest tests.test_contracts -v            # 60 tests against the real modules
python3 -m unittest discover -s tests -p 'adversarial*.py' -v   # 104: replay, trust, breaker-clock, liveness lenses
cd review && python3 agent_bus_regression_tests.py --repo ..   # the reviewer's 21 checks
```

`agent_base.py` needs `comms.py`; `bd` is optional. Subclass `BaseAgent`, override `handle(req) -> dict`
(return a dict with a boolean `ok`; add `failure_class: "rejection"` when the request, not the agent,
was wrong) and optionally `idle_cycle()`, then call `run()`. Do not override `tick()`. Requires POSIX
for the single-consumer `fcntl` lock.

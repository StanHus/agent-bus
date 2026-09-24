# agent-bus contract

What the bus and the base agent guarantee, the failure that each guarantee is
about, and the test that pins it. Reviewer checks are `R<n>` in
`review/agent_bus_regression_tests.py` (run with `--repo ..` from `review/`);
repository tests are `T:<name>` in `tests/test_contracts.py`; adversarial
tests (rounds 1 to 3: replay, trust, breaker-clock, liveness lenses) are
`A:<name>` in `tests/adversarial_*.py`. Run the adversarial files with
`python3 -m unittest discover -s tests -p 'adversarial*.py'` and
`python3 -m unittest tests.adversarial_breaker_clock`.

This is **at-most-once execution per event id**, not exactly-once. See
"What is not guaranteed" at the end before relying on it for anything that
moves money, sends email, or launches a training run.

## Files an operator reads

All under `AGENTS_ROOT/<agent>/` unless stated.

| Path | Meaning |
|---|---|
| `inbox/<eid>.json` | Pending, or handled but not yet acknowledged. A request file keeps its name until it is acknowledged (deleted). |
| `quarantine/<name>` | A request that could not be parsed or failed envelope validation, or a non-regular inbox entry. Bytes are exactly what arrived; on a name collision the file is `<name>.<ts>`, then `<name>.<ts>.<n>`. A name too long to carry the sidecar suffix is stored as `<first 180 chars>~<16 hex of sha256(name)>`; the sidecar's `original` keeps the full name. Never overwritten. |
| `quarantine/<name>.reason.json` | `{ts, agent, original, quarantined_as, reason_code, detail}`. Codes: `decode_error` (also a document nested too deep to decode), `oversized`, `not_a_regular_file`, `not_an_object`, `event_id_mismatch`, `bad_identifier`, `bad_kind`, `unregistered_caller`. Created exclusively before the move, so the sidecar name is reserved and can never replace a quarantined request. |
| `state/outcomes/<eid>.json` | Durable per-message record: `status` (`executing`, `recorded`, `delivered`, `acknowledged`), `fingerprint` (sha256 of the request as the handler sees it), `owner {pid, claimed_at}` (who claimed it), `result`, `outcome_class`, `attempts {execute, deliver, ack}`, `reexecuted_from` when an idempotent kind was re-run over an `executing` or unreadable record. Anything that does not read as an object with one of the four statuses is treated as unreadable. |
| `state/breaker.json` | Written when the breaker trips: `state, opened_at, cooldown_s, consec, threshold, reason`. |
| `state/restart_intent.json` | A `heal: respawn` in flight: `accepted` -> `exiting` -> `completed` (set at next boot). |
| `state/dream_slots.json` | `<date>T<HH:MM>` -> `{status, attempts, ts, late}` for the last 7 days. `status` is `completed`, `failed` or `missed`; `running` appears only while a dream is in progress (the attempt is written before the dream runs) and reads as a failed attempt if the process died inside it. A wrong-shaped entry is repaired or dropped (audit `dream_slot_entry_invalid`), never fatal. |
| `state/health.json` | Rewritten every tick. See "Monitoring". |
| `state/current_op.json` | Exists only while a handler is running: `{event_id, kind, caller, started, deadline_s}`. One left behind by a hard crash is removed at boot (audit `current_op_stale_cleared` with its age). If it cannot be written before a handler runs the handler still runs and `current_op_write_failed` is audited (the hung-handler signal is off for that message); one that cannot be removed afterwards is audited `current_op_unremovable`. |
| `state/consumer.lock`, `state/consumer.owner.json` | flock held by the running daemon; owner file is for humans. A lock file that cannot be opened, or a volume whose `flock` fails with anything but a held-lock errno, is audited `consumer_lock_unavailable` and the daemon runs without it. |
| `logs/audit_fallback.jsonl` | Audit records the agent could not deliver to `AUDIT_DIR` (the primary write raised or `comms.audit` returned `False`), each with an `audit_error` note. |
| `AGENTS_ROOT/_audit_fallback/<date>_<agent>.jsonl` | Same, at the bus layer (`comms.audit`). |
| `AGENTS_ROOT/_trust/operator.key` | If present, `heal` and `invoke_skill` requests must carry a valid HMAC `auth` field signed for this agent. An empty or unreadable key refuses every privileged request (audit `privileged_key_unusable`) and health reports `privileged_auth_key: empty|unreadable`. Unreadable includes anything that is not a regular file of at most 64 KiB (a FIFO, symlink, device or directory): the key is `lstat`ed and opened `O_NOFOLLOW|O_NONBLOCK`, never followed and never blocking. |
| `AGENTS_ROOT/_registry.json` | Optional list of agent names; `send_directive` refuses targets not listed, `drain_inbox` quarantines requests whose caller is not listed (`unregistered_caller`), `respond` refuses replies to unlisted callers. Names beside the agent directories start with `_`, which no agent name may. |

## Message lifecycle

```
sender: exclusive-create <eid>.json.tmp in <agent>/inbox (a planted link is unlinked, never written through), rename to <eid>.json
agent tick:
  drain_inbox        list inbox (cannot list -> audit inbox_unlistable, drain nothing)
                     lstat (regular file? size?) -> bounded read -> decode -> validate envelope, registry
                                                                   (quarantine, or leave on read error)
  _process_message   fingerprint = sha256 of the request with event_id := filename stem, caller defaulted
                     load record (not an object with a known status -> `corrupt`)
                     new           -> claim: exclusive create of record {status: executing, owner: {pid, claimed_at}}
                                      (exists -> another consumer owns it, leave the file; cannot write -> do not run, stop batch)
                                      run handler under one exception boundary
                                      validate result
                                      write record {status: recorded, result}
                     recorded      -> skip execution, deliver (this caller's outbox already failed this tick -> leave for next tick)
                     delivered     -> skip execution; the recorded reply still in the caller's outbox -> acknowledge only,
                                      reply gone or replaced by another document (duplicate after a lost ack record)
                                      -> republish, acknowledge
                     acknowledged  -> duplicate resend: replay recorded result, acknowledge
                     executing, owner alive and claim inside claim_lease_s -> another consumer's live claim: leave the file
                     executing (owner gone) or corrupt -> crash window: reply `uncertain`, do not run
                                      (unless kind in IDEMPOTENT_KINDS: re-run, record replaced, `reexecuted_from` set)
                     other fingerprint -> reply rejection, acknowledge, keep the original record
                     deliver       respond(caller, eid, result) -> {status: delivered}
                                   (this caller's outbox unwritable -> attempts.deliver++, audit, next message runs;
                                    the class the handler earned this tick still reaches the breaker)
                     acknowledge   unlink request -> {status: acknowledged}   (already gone -> acknowledged)
                     bookkeeping   breaker, bead, audit directive_handled
                     budget        handled messages count; a retried reply/ack or a skipped file does not;
                                   a file passed over because the breaker is open (or the half-open probe
                                   is already in) is charged to neither the message nor the time budget
  idle_cycle (skipped while breaker open or half open), _maybe_dream
  finally: heartbeat once (a helper exception is audited heartbeat_failed), then state/health.json
  if the respawn request itself is acknowledged (this tick, or already on disk from an earlier one):
                                   mark intent exiting, os._exit(0)
```

## Guarantees

### Transport (comms.py)

1. **Identifiers are not paths.** Agent names, callers and event ids must match
   `[A-Za-z0-9._-]{1,128}` and not start with `.`; agent names (own name,
   caller, target) additionally must not start with `_`, the prefix of the
   bus's own files beside the agent directories (`_registry.json`, `_trust`,
   `_audit_fallback`, `_heartbeats_local`). `inbox_dir`, `outbox_dir`,
   `respond`, `send_directive`, `heartbeat`, `write_bus_health` and
   `alert_main` validate before any `mkdir` or write and raise `ValueError`
   (`heartbeat` returns `(False, False)`). The agent directory and its
   `inbox`/`outbox` must be real directories under `AGENTS_ROOT`: a symbolic
   link in either position raises `PermissionError` and nothing is written.
   Failures covered: `caller: "../escaped"` writing outside the root,
   `caller: "_registry.json"` turning the registry into a directory, a
   symlinked outbox redirecting replies. R17, R18; T:test_validate_identifier_rules,
   T:test_dirs_validate_before_mkdir, A:test_reserved_root_names_cannot_be_used_as_a_caller,
   A:test_heartbeat_and_bus_health_do_not_accept_path_like_agent_names,
   A:test_respond_refuses_an_outbox_that_is_a_symlink_out_of_agents_root.
2. **The transport event id wins.** `respond` writes `{**payload, "event_id": eid}`;
   the temp file is `<eid>.json.tmp` in the same outbox, created exclusively
   (`O_CREAT|O_EXCL|O_NOFOLLOW`): an entry already under that name (a leftover
   of an interrupted reply, or a link planted by the caller) is unlinked as a
   directory entry and the create retried once, so the agent's uid never
   writes through someone else's link. When `_registry.json` exists, a reply
   to an unlisted caller raises `ValueError`. R16;
   T:test_respond_tmp_name_keeps_dotted_ids_in_outbox,
   A:test_respond_does_not_follow_a_symlinked_tmp_in_the_caller_outbox.
3. **drain_inbox never deletes, and only opens regular files.** Each entry is
   `lstat`ed first: a symlink, FIFO, device or directory named like a request
   is quarantined `not_a_regular_file` without being opened (a FIFO would
   block the tick forever, a symlink would read and later unlink something
   else); a size over 1 MiB is quarantined `oversized` before any byte is
   read, and the read itself (`O_NOFOLLOW|O_NONBLOCK`, through the fd) stops
   at the limit. A read `OSError` leaves the file and audits
   `inbox_read_error`; the next tick retries. An inbox directory that cannot
   be listed drains nothing and audits `inbox_unlistable`; health then reports
   `inbox_pending: null`, never `0`. Bad input (undecodable, including a
   document nested too deep for the decoder, not an object, envelope
   `event_id` != filename stem, invalid caller, non-string or oversized kind,
   caller not in `_registry.json` when that file exists) is moved
   byte-for-byte into `quarantine/` with a sidecar and audited
   `inbox_quarantined`, and never stops the batch. The sidecar is created
   exclusively before the move, so quarantined bytes are never overwritten:
   not by a later sidecar of the same name, not by a same-second collision
   (`<name>.<ts>.<n>`). A name too long to take the `.reason.json` suffix
   (NAME_MAX) is quarantined under a bounded `<prefix>~<hash>` name so bad
   input leaves the inbox exactly once. A registry that exists but cannot be
   read drains nothing (audit `registry_unreadable`). The filename stem is the
   event id. R7, R21;
   T:test_transient_read_error_is_left_in_place_not_quarantined,
   T:test_envelope_mismatch_and_bad_caller_are_quarantined,
   T:test_drain_inbox_never_unlinks, T:test_quarantine_collision_keeps_both_files,
   T:test_oversized_message_is_quarantined,
   A:test_symlinked_request_in_the_inbox_is_not_followed,
   A:test_fifo_named_like_a_request_does_not_hang_the_tick,
   A:test_oversized_request_is_rejected_by_size_before_it_is_read,
   A:test_quarantine_sidecar_cannot_overwrite_a_quarantined_request,
   A:test_quarantine_collision_within_one_second_keeps_every_file,
   A:test_unknown_caller_is_refused_when_a_registry_exists,
   A:test_deeply_nested_request_does_not_block_later_messages,
   A:test_bad_request_with_a_maximal_filename_is_quarantined_not_retried_forever,
   A:test_unlistable_inbox_is_audited_not_reported_as_empty. A request without
   `kind` is still delivered to `handle()` (the token protocol has none):
   T:test_missing_kind_still_reaches_handle.
4. **audit never raises.** Primary `AUDIT_DIR`, then `AGENTS_ROOT/_audit_fallback/`,
   then stderr; returns `True` only for a primary write. Fields json cannot
   encode even with `default=str` (tuple keys, circular structures) are written
   as `fields_repr` with `audit_serialization_error`.
   T:test_comms_audit_falls_back_and_never_raises,
   A:test_audit_helpers_never_raise_on_unserializable_fields.
5. **heartbeat reports what it wrote**: `(local_ok, icloud_ok)`. T:test_heartbeat_reports_what_it_wrote.
6. **send_directive validates target, caller and event id**, honours
   `_registry.json` when present, and creates its temp file exclusively
   (`O_CREAT|O_EXCL|O_NOFOLLOW`, one retry after unlinking a pre-existing
   entry), so a `<eid>.json.tmp` link planted in the target inbox by another
   sender is never written through. T:test_send_directive_validates_and_honours_registry,
   A:test_send_directive_does_not_write_through_a_planted_symlink_in_the_target_inbox.

### Execution (agent_base.py)

7. **One exception boundary.** `heal`, `dream`, `plan`, `invoke_skill` and the
   subclass `handle` run inside `_dispatch`; any `Exception` becomes
   `{ok: false, error: "handler raised: ..."}` and an audit `handler_exception`
   with event id, exception type and traceback (capped at 4000 chars), also
   when the exception's own `__str__` raises. `BaseException` (process exit,
   keyboard interrupt) propagates. Handlers receive a copy of the request with
   `event_id` set to the filename stem and `caller` defaulted, so a
   consequential handler always has the transport id to pass downstream.
   R2, R8; T:test_02_subclass_exception_becomes_error_reply_with_traceback,
   T:test_08_builtin_exception_is_contained, A:test_dispatch_contains_exception_whose_str_raises.
8. **Directive arguments are checked before use.** `plan` needs a string title,
   a list of string steps, a string rationale, all UTF-8 encodable (JSON's
   lone-surrogate escapes are text the plan file cannot store), and is called
   with keyword arguments; `invoke_skill` needs a non-empty string skill, a
   list `args`, a dict `kwargs` with string keys none of which is `name` or
   `agent` (those would shadow the skill name and the agent identity the bus
   injects). Violations reply `failure_class: rejection` and never reach the
   handler. R5, R9; T:test_05_plan_dispatch_uses_keywords_and_validates_arguments,
   T:test_09_invalid_skill_arguments_are_rejected,
   A:test_invoke_skill_kwargs_that_collide_with_transport_parameters_are_rejected,
   A:test_plan_with_unencodable_text_is_a_rejection_not_a_counted_failure.
9. **Results are validated once.** Non-dict -> `ok: false`. `ok` that is not a
   boolean (the string `"false"` included) -> `ok: false` with the schema error.
   Non-string `error` -> `str(error)[:500]`. A payload `event_id` is dropped.
   A result `json.dumps(default=str)` cannot encode (tuple keys, or a value
   whose `__str__` raises: every exception from the encoder counts, not only
   `TypeError`/`ValueError`) -> `ok: false`, `error: "handler result is not
   JSON-serializable: ..."`, `handler_ok` kept, class `failure`; it is
   answered in the same tick, never left as a crash window. Every downstream
   consumer (reply, breaker, bead, audit, record) uses the validated form;
   published `ok` is always a JSON boolean. R10, R11, R19;
   T:test_validate_result_normalises, A:test_unserializable_success_result_is_not_reported_as_uncertain,
   A:test_result_value_whose_str_raises_is_answered_in_the_same_tick.
10. **Failure classes.** `success` resets the breaker counter. `rejection`
    (the request was wrong: default `handle`, unknown heal action, argument
    checks, identity reuse, unauthorized) and `uncertain` never count.
    `failure` (exception, plain `ok: false`, contract violation) and `storage`
    (the agent's own state volume: the record, including the `delivered` and
    `acknowledged` writes, or the acknowledgement could not be written) count.
    `delivery` (this caller's outbox could not be written: permissions, quota,
    a symlinked or refused destination) is audited `response_delivery_failed`,
    bumps `attempts.deliver`, is retried from the record next tick and does
    not count. The breaker observes the class the handler earned in the tick
    the handler ran, whatever happens to the reply: a failure whose reply
    cannot be delivered still counts (once, never again on redelivery), a
    success still resets, `storage` still overrides. One caller cannot trip
    the agent-wide breaker, keep it from tripping, or starve the others:
    redelivery retries are not charged to the message budget and, after the
    first delivery failure to a caller in a tick, its remaining redeliveries
    wait for the next tick (audit `redelivery_deferred`, once per caller per
    tick). Subclasses opt in to `rejection` by returning
    `failure_class: "rejection"`. R2, R6; T:test_rejections_do_not_count_toward_breaker,
    T:test_unknown_heal_action_is_a_rejection,
    A:test_record_write_failure_after_the_reply_counts_as_storage,
    A:test_one_callers_unwritable_outbox_does_not_block_other_callers,
    A:test_handler_failure_counts_toward_the_threshold_when_the_reply_cannot_be_delivered,
    A:test_delivery_retries_do_not_consume_the_whole_tick_budget.

### Durable outcome and dedup

11. **Record before running, record before replying.** `state/outcomes/<eid>.json`
    is written `executing` before dispatch, carrying `owner {pid, claimed_at}`,
    and `recorded` (with the result) before `respond`. The first `executing`
    write is the claim and is an exclusive create (private temp file
    hard-linked into place; exclusive `open` where the filesystem has no hard
    links, and if the bytes then cannot be written the created entry is
    unlinked again, so a failed claim never leaves an empty or partial record
    that the next tick would answer `uncertain` without ever executing): if
    the record already exists another consumer owns the message, the file is
    left alone and `claim_conflict` is audited. A re-execution of
    an idempotent kind over an `executing` or unreadable record is not a
    first claim: it replaces the record (tmp + rename, `reexecuted_from`
    set), so a corrupt record can never wedge a request. Later writes are
    tmp + rename. If the `executing` write fails the handler is not run and
    the batch stops with every file left in place.
    T:test_executing_record_write_failure_stops_before_executing,
    A:test_executing_claim_is_not_exclusive_so_a_stale_reader_double_executes,
    A:test_idempotent_kind_with_corrupt_record_is_reexecuted_not_stuck,
    A:test_failed_exclusive_create_claim_does_not_leave_a_record_that_answers_uncertain.
12. **Reply failure does not repeat work or block others.** If `respond` raises,
    the record stays `recorded` with `attempts.deliver` incremented and the
    request stays in the inbox; the batch continues with the next message; the
    next tick redelivers the recorded result and acknowledges without calling
    the handler. R12; T:test_12_response_write_failure_does_not_repeat_handler,
    A:test_undeliverable_caller_does_not_starve_other_callers.
13. **Acknowledgement failure does not repeat work or the reply.** If `unlink`
    raises, the record stays `delivered`; the next tick only retries the unlink
    while the recorded reply is still in the caller's outbox (the file's
    content is compared with the record; a different document under that
    name is not the reply). A request file that is
    already gone when the unlink runs counts as acknowledged (audit
    `ack_already_gone`). R13; T:test_13_unlink_failure_does_not_repeat_handler,
    A:test_request_removed_before_ack_is_treated_as_acknowledged.
14. **Duplicates.** Same event id, same input fingerprint (sha256 of the
    canonical JSON of the request as the handler sees it: `event_id` forced to
    the filename stem and `caller` defaulted, so spelling out or omitting
    those transport fields does not change the input) -> the recorded result
    is republished, the file acknowledged, audit `duplicate_replayed`. This holds from `acknowledged` and from
    `delivered` when the recorded reply is no longer in the caller's outbox
    (the process died between the unlink and the `acknowledged` write, the
    caller consumed the reply and resent), also when another document sits in
    the slot, such as the rejection of an intervening same-id misuse: the
    republish overwrites it atomically. Same id, different fingerprint -> reply
    `failure_class: rejection`, acknowledge, original record untouched, audit
    `identity_reuse`. Neither runs the handler. T:test_duplicate_same_input_returns_recorded_result,
    T:test_duplicate_different_input_is_rejected,
    A:test_duplicate_after_crash_between_ack_and_record_is_replayed,
    A:test_duplicate_that_differs_only_in_transport_defaults_is_replayed_not_rejected,
    A:test_same_input_duplicate_after_lost_ack_record_is_replayed_even_when_a_foreign_reply_occupies_the_slot.
15. **Crash window.** A record found in `executing` whose owner is gone, or a
    record that cannot be read as one (truncated, `null`, a list, `{}`, an
    unknown status, or a `recorded`/`delivered`/`acknowledged` record whose
    `result` is present but not an object and so can never be replayed),
    means the previous process stopped between running the
    handler and recording the outcome. The reply is `{ok: false, outcome:
    "uncertain", event_id, kind, status: "uncertain"}`, an `OUTCOME_UNCERTAIN`
    alert goes to the operator, the request is acknowledged and the handler is
    not run again, unless the kind is listed in `IDEMPOTENT_KINDS`, in which
    case it re-executes and audits `reexecuting_idempotent`. An `executing`
    record whose owner pid is alive and whose claim is younger than
    `claim_lease_s` (3600 s) is another consumer's live claim, not a crash: the
    file is left alone, `claim_in_progress` is audited, no reply, no alert.
    (When the owner pid is this process, the claim is live only while
    `state/current_op.json` names the event id.)
    T:test_crash_between_side_effect_and_record_is_uncertain_not_rerun,
    T:test_crash_recovery_reexecutes_only_idempotent_kinds,
    A:test_non_object_outcome_record_is_uncertain_not_stuck_or_rejected,
    A:test_live_claim_by_another_consumer_is_not_treated_as_a_crash_window,
    A:test_recorded_record_with_a_non_object_result_is_not_retried_forever.
16. **One consumer per agent name.** `__init__` takes `fcntl.flock(LOCK_EX|LOCK_NB)`
    on `state/consumer.lock` and holds it for the process lifetime; a second
    daemon fails in `__init__` with `RuntimeError` (the held-lock errnos
    `EWOULDBLOCK`/`EAGAIN`/`EACCES`). The kernel releases the lock on crash, so
    nothing is renewed or taken over. Independently, the exclusive claim of
    guarantee 11 means a message is never run by two consumers even when one
    of them read the outcomes directory before the other's claim landed, and
    the owner data of guarantee 15 means a second consumer that meets a live
    claim backs off instead of answering uncertain. Without `fcntl`, when the
    lock file cannot be opened (a read-only `state/` with no lock file yet), or
    when the volume does not support locking (`ENOLCK`, `ENOTSUP`, ...), the
    agent audits `consumer_lock_unavailable` and runs; it is never misreported
    as a second consumer. T:test_overlapping_consumer_is_refused,
    T:test_overlapping_consumer_cannot_double_execute_claimed_message,
    A:test_boot_survives_read_only_state_dir_without_a_lock_file,
    A:test_flock_unsupported_on_the_volume_is_reported_not_misdiagnosed.

### Circuit breaker

17. **The batch stops at the threshold.** After the fifth consecutive counted
    failure the current message is still recorded, replied to and acknowledged;
    then `_circuit_breaker_open()` is true at the top of the next iteration and
    the remaining files stay untouched. `idle_cycle` is skipped while open and
    while half open: half open admits one message probe, not unbounded idle
    work, and an idle outcome cannot close the breaker. The probe bound is on
    messages admitted, not on verdicts received: once one message has been
    admitted while half open the rest of the batch waits (heal excepted), so
    a probe that ends as a rejection, an uncertain or a duplicate reply lets no
    second handler run in that tick; the breaker stays half open and the next
    tick admits the next probe. R6;
    T:test_06_breaker_stops_batch_at_threshold_after_acknowledging,
    A:test_half_open_does_not_run_idle_work_on_every_tick_without_a_probe,
    A:test_half_open_admits_one_handler_run_even_when_the_first_is_a_rejection.
18. **State machine on a monotonic clock.** `closed -> open` on trip
    (`_circuit_breaker_pause_until = time.monotonic() + cooldown`, base 300 s);
    `open -> half_open` when the cooldown passes; half open admits exactly one
    probe: success closes, resets the cooldown and clears `reason`; a counted
    failure (a failing handler or the probe's own `executing` record not being
    writable alike) reopens with the cooldown doubled (cap 3600 s). The
    probe's outcome reaches the breaker in the tick the probe runs, also when
    its reply cannot be delivered. There is no `open -> open` edge: a counted
    failure while open (a `heal` that fails) is counted but does not restart
    the cooldown or record a second trip. `state/breaker.json` records each
    trip with the threshold; the alert body has real newlines and carries the
    threshold value on its own `**threshold**` line, whatever the reason text
    says (a probe reopening on a storage failure names no count). R3, R19;
    T:test_breaker_half_open_probe_reopens_with_doubled_cooldown_then_closes,
    T:test_19_error_object_still_trips_breaker, T:test_trip_breaker_alert_has_real_newlines,
    A:test_counted_failure_while_open_does_not_restart_the_cooldown,
    A:test_half_open_probe_whose_record_cannot_be_written_reopens,
    A:test_health_breaker_reason_is_cleared_when_the_breaker_closes,
    A:test_half_open_probe_failure_reopens_even_when_its_reply_cannot_be_delivered,
    A:test_successful_probe_closes_the_breaker_once_its_reply_is_delivered,
    A:test_reopen_alert_from_a_storage_probe_carries_the_threshold_value.
19. **Recovery stays reachable.** While open, `kind: heal` requests are still
    processed; everything else waits in the inbox. The time spent walking past
    paused files is refunded to the tick's time budget (nothing was done with
    them), so a heal queued behind a backlog larger than `tick_budget_s` can
    walk is still reached in the same tick; the walk is bounded by the inbox
    size, not the budget. R3; T:test_heal_is_delivered_while_breaker_open,
    A:test_heal_stays_reachable_while_open_behind_a_paused_backlog.

### Restart and self-heal

20. **Respawn is deferred and acknowledged.** `healing_handler` writes
    `state/restart_intent.json` (`accepted`, carrying the transport event id
    even when the envelope omitted it), replies `{ok: true, healed:
    "respawn", deferred: true}` and returns. `tick` records, replies,
    acknowledges, heartbeats, marks the intent `exiting`, then calls
    `os._exit(0)`, and only once the respawn request's own durable record is
    `acknowledged`: another message reaching `acknowledged` while the respawn
    reply is still pending does not exit, and neither does the transient
    rejection record of a different-input duplicate under the same event id.
    The exit does not depend on the helpers: an exception escaping the
    heartbeat is audited `heartbeat_failed` and the exit still happens, and
    if it did not (the process was interrupted) the next tick finds the
    respawn's record `acknowledged` with the intent still pending and exits
    then, before draining. On the next boot (`__init__` and the
    top of `run()`) an intent in `accepted`/`exiting` is marked `completed`
    and audited `restart_intent_reconciled`; it is never re-executed. Boot
    survives an intent that is not a JSON object (audit `restart_intent_corrupt`)
    and a read-only `state/` (audit `restart_intent_write_failed`, intent kept).
    A `state/current_op.json` left by a hard crash is removed at boot (audit
    `current_op_stale_cleared`); one that cannot be removed after a handler
    returns is audited `current_op_unremovable` in the same tick. R15;
    T:test_15_respawn_is_acknowledged_then_exits_then_reconciles_on_boot,
    A:test_exit_waits_for_the_respawn_request_itself_to_be_acknowledged,
    A:test_restart_intent_carries_the_transport_event_id,
    A:test_boot_survives_non_dict_restart_intent,
    A:test_boot_survives_unwritable_state_dir_when_intent_is_pending,
    A:test_current_op_left_by_a_hard_crash_is_cleared_on_boot,
    A:test_rejected_identity_reuse_of_the_respawn_id_does_not_trigger_the_exit,
    A:test_current_op_that_cannot_be_removed_is_not_left_silently,
    A:test_current_op_that_cannot_be_written_is_audited,
    A:test_heartbeat_exception_does_not_lose_the_accepted_restart.
21. **soft_reset does not destroy in-flight writes.** Only `*.tmp` files in the
    agent's own inbox/outbox older than `stale_tmp_age_s` (600 s) are removed,
    each audited with its age. T:test_soft_reset_removes_only_stale_tmp_files.
22. **Privileged kinds.** When `AGENTS_ROOT/_trust/operator.key` exists, `heal`
    and `invoke_skill` must carry `auth = HMAC-SHA256(key, message)` where
    `message` is the canonical JSON array `[target, event_id, kind, action,
    caller, args, kwargs]` (sorted keys, no whitespace; `args` default `[]`,
    `kwargs` default `{}`; `comms.signed_message`, produced by
    `comms.sign_directive(..., target=<agent>)`). The receiving agent verifies
    with its own name as `target`, so a signature is valid for exactly one
    agent: the same signed bytes copied into another agent's inbox are
    rejected there, and a signed `invoke_skill` cannot be re-targeted by
    rewriting the pending file. A missing, wrong or malformed signature
    (non-string, non-ASCII) is a `rejection` and never counts. An empty or
    unreadable key refuses every privileged request as a `rejection` (audit
    `privileged_key_unusable`). Without the key they run, audit
    `privileged_unauthenticated`, and `health.privileged_auth_enforced` is
    false so the gap is visible; `health.privileged_auth_key` says
    `ok|absent|empty|unreadable`, and `enforced` is true only for `ok`.
    The key must be a regular file of at most 64 KiB: it is `lstat`ed and
    read `O_NOFOLLOW|O_NONBLOCK` through an fd, so a FIFO, symlink, device or
    directory at that path is `unreadable` and never blocks the tick.
    T:test_privileged_kinds_require_hmac_when_key_exists,
    A:test_fifo_operator_key_does_not_block_the_tick,
    A:test_signed_invoke_skill_arguments_cannot_be_altered_in_transit,
    A:test_signed_privileged_request_cannot_be_replayed_to_a_different_agent,
    A:test_non_ascii_auth_is_a_rejection_not_a_counted_failure,
    A:test_empty_operator_key_is_not_reported_as_enforced_auth.

### Liveness and telemetry

23. **Heartbeat exactly once per tick, on every path**: success, handler
    exception, reply failure, quarantine, breaker open, respawn exit. It is the
    only call site and it sits in `tick`'s `finally`, inside its own exception
    boundary: an exception escaping the helper is audited `heartbeat_failed`,
    `heartbeat_written` reports `[false, false]`, and the tick's outcome (a
    deferred exit included) is unchanged. R1, R3, R4, R14;
    T:test_heartbeat_once_on_every_path.
24. **Audit cannot kill the loop.** All agent-side audits go through
    `_audit_safe`, which attaches tracebacks and event ids, falls back to
    `logs/audit_fallback.jsonl` then stderr, and never raises: not for an
    exception whose `__str__` raises, not for fields json cannot encode.
    It returns `True` only when the primary audit write succeeded: a `False`
    from `comms.audit` (record diverted to `_audit_fallback/`) is a degraded
    audit trail exactly like an exception, sets `health.audit_degraded` and
    appends the record to `logs/audit_fallback.jsonl`; the flag clears on the
    next primary write that succeeds, so health reflects the most recent
    audit write. `run()` reaches `time.sleep` when both `tick` and `audit` fail. R20;
    T:test_20_audit_failure_does_not_terminate_run_loop,
    A:test_audit_safe_never_raises_on_unprintable_exception,
    A:test_audit_falling_back_is_visible_in_health_and_in_the_return_value.
25. **Per-message failures are isolated.** An exception inside
    `_process_message` itself (for example the state volume failing) is audited
    `message_exception`, the file is left for the next tick and the following
    messages still run. T:test_message_exception_is_audited_and_next_message_runs.
26. **Per-tick budget.** At most `max_messages_per_tick` (50) handled messages
    (a handler run, an uncertain or duplicate reply, a rejection, or a message
    whose processing raised) and `tick_budget_s` (default `max(5, 0.8 *
    cadence_s)`) seconds of draining; a retried reply or acknowledgement of an
    earlier tick's outcome and a file left untouched (live claim, lost claim
    race, deferred redelivery) are not charged to the message count, only to
    the time; a file passed over because the breaker is open, or because the
    half-open probe is already in, is charged to neither (guarantee 19). The
    remainder stays in the inbox, `tick_budget_exhausted` is
    audited with processed and remaining counts, and idle/dream/heartbeat still run.
    T:test_per_tick_message_budget_leaves_remainder_then_runs_maintenance,
    T:test_per_tick_time_budget_stops_draining.
27. **Dream slots.** Windows are `[start, start + 5 min]` (timedelta) and the
    previous UTC day's slots are examined as well as today's, so a 23:58
    window still runs at 00:01; when the file holds older slots the walk
    starts at the oldest retained day (at most 7 back), so the slots of whole
    days skipped by downtime are recorded `missed` rather than left out. A
    fresh file starts at yesterday. The attempt is persisted (`running`,
    `attempts + 1`) before `dream_sequence` runs, so a dream that kills the
    process still counts and a crashing dream is bounded like a failing one:
    at most three attempts per slot; a slot found `running` at the next load
    is a failed attempt (audit `dream_sequence_interrupted`). If the attempt
    cannot be persisted the dream does not run that tick (audit
    `dream_slots_write_failed`). Completed slots persist in
    `state/dream_slots.json` so a restart inside the window does not repeat
    one; a slot missed while busy is run once later the same UTC day when
    `dream_catch_up` is true; a slot never run when its UTC day ends (or with
    catch-up off) is recorded as `missed`, while a slot that ran and failed
    keeps its `failed` record, so the file shows every slot of the last 7
    days as completed/failed/missed. A wrong-shaped file or entry is repaired
    (a bare status string) or dropped (audit `dream_slot_entry_invalid`) and
    never disables dreaming. One dream per tick.
    T:test_maybe_dream_at_minute_58_uses_timedelta_and_persists_slot,
    T:test_maybe_dream_missed_slot_catch_up_and_retry_limit,
    A:test_dream_window_that_crosses_midnight_still_runs_after_midnight,
    A:test_slot_never_run_before_the_day_ends_is_recorded_as_missed,
    A:test_dream_attempt_is_durable_before_the_run_so_a_crashing_dream_is_bounded,
    A:test_dream_attempt_is_persisted_before_the_run_so_a_crashing_dream_stops_after_three,
    A:test_failed_dream_slot_is_not_rewritten_as_missed_when_its_day_ends,
    A:test_wrong_shaped_dream_slot_entry_does_not_disable_dreaming,
    A:test_slots_of_a_day_skipped_by_downtime_are_recorded_as_missed.
28. **bd is optional.** `BD_STATUS` is `stub`, `absent` or `live`; only `live`
    receives bead traffic; the status is audited once at startup and shown in
    health. T:test_bd_stub_is_reported_and_never_called.

## Monitoring

`state/health.json` carries four independent signals; the mesh doctor should
read them separately rather than folding them into one "up" flag:

| Signal | Where | Rule |
|---|---|---|
| alive | heartbeat file freshness; `health.ts` | stale -> process dead or stuck in a handler |
| making progress | `health.last_progress_ts` | set when a message reaches `acknowledged` |
| breaker | `health.breaker.state`, `until_s`, `reason` | `open`/`half_open` -> paused, see `state/breaker.json` |
| operation overdue | `state/current_op.json` age vs `deadline_s` | file present and older than the deadline -> the handler is hung; the agent cannot report this itself. A file left by a hard crash is removed at the next boot, so this signal never fires for a healthy restarted daemon |

Also: `heartbeat_written` (`[local, icloud]`), `audit_degraded`,
`inbox_pending` (`null` when the inbox cannot be listed: see `inbox_unlistable`
in the audit), `quarantined_total`, `restart_pending`, `privileged_auth_enforced`,
`privileged_auth_key` (`ok|absent|empty|unreadable`), `bd`.

## Runbook

* **Breaker stuck open**: fix the dependency, then either wait for `until_s`
  (one probe is admitted, success closes it) or restart the daemon (state is
  in memory; `state/breaker.json` is a record, not an input). The probe is a
  message: with an empty inbox the breaker stays `half_open` and idle work
  stays paused until a request arrives, so send one (`heal: rewrite_north_star`
  is harmless) or restart.
* **A caller never receives replies** (`response_delivery_failed` repeating,
  `attempts.deliver` climbing): that caller's outbox is unwritable, symlinked
  or unregistered; the work is done and recorded, other callers are unaffected.
  Fix the destination; the next tick redelivers from the record.
* **`claim_conflict` or `claim_in_progress` audited**: two consumers saw the
  same request; only one ran it (the other lost the exclusive claim, or met a
  claim whose owner pid is still alive). Check `state/consumer.owner.json`
  and the fcntl lock; on a platform without fcntl this is the expected
  protection at work. A claim whose owner is alive for longer than
  `claim_lease_s` (1 h) is treated as a crash window on the next tick.
* **`redelivery_deferred` audited**: that caller's outbox failed earlier in
  the tick; its other pending replies wait one cadence instead of being
  retried in the same batch.
* **`privileged_key_unusable` audited / `health.privileged_auth_key` not
  `ok`**: `_trust/operator.key` is empty or unreadable; every heal and
  invoke_skill is refused until the key is fixed. Signatures must be made with
  `sign_directive(..., target=<agent name>)`; one signed for another agent is
  rejected here.
* **`consumer_lock_unavailable` audited**: the daemon runs without the flock
  (no fcntl, a lock file it cannot open, or a volume without locking). Make
  sure a second daemon is not started by hand; the exclusive claim and the
  owner check on the outcome record are the remaining protection.
* **Quarantined request**: read the `.reason.json`, fix the sender, move the
  file back into `inbox/` under a valid `<eid>.json` name if it should run.
* **`OUTCOME_UNCERTAIN` alert**: check downstream whether the side effect
  happened; the record under `state/outcomes/` has the fingerprint and times.
  Resubmit under a new event id only after reconciling.
* **Second daemon refuses to start** (`another consumer already holds ...`):
  the first one is alive. `state/consumer.owner.json` names its pid.
* **Force-close a lingering `restart_intent.json`**: not needed; the next boot
  marks it completed.

## What is not guaranteed

* **Exactly-once.** Between a handler's external side effect and the
  `recorded` write the process can die. The next tick replies `uncertain` and
  does not re-run, so the effect may have happened once with no success reply.
  Consequential handlers must pass `event_id` downstream as an idempotency key,
  or be listed in `IDEMPOTENT_KINDS` and tolerate a second run.
* **Hard handler deadlines.** Handlers run on the main thread; a hung handler
  is visible (`current_op.json` age, stale heartbeat) but not interrupted.
* **Authorization without a key.** Filesystem permissions on the inbox are the
  real boundary until `_trust/operator.key` is deployed.
* **Cross-platform locking.** The consumer lock uses `fcntl`; elsewhere it is
  reported unavailable and the exclusive claim of guarantee 11 (plus the
  owner-aware `executing` check of guarantee 15) is all that stands between
  two consumers. The owner liveness probe is `kill(pid, 0)` on POSIX; on other
  platforms an owner inside the lease is assumed alive, so a stale claim there
  is answered uncertain only after `claim_lease_s`.
* **Old signatures.** The signed message changed shape (canonical JSON array
  including the target agent); signatures made by the previous revision, or
  by `sign_directive` without `target`, are rejected everywhere.
* **Registry-less identity.** Without `_registry.json` any well-formed agent
  name is accepted as a caller and gets an outbox directory; the registry is
  the documented way to close that.

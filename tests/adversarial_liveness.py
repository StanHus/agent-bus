"""Adversarial liveness tests: heartbeat, audit, restart, helper failures.

    /opt/homebrew/bin/python3 -m unittest tests.adversarial_liveness -v

Each test here targets a guarantee in CONTRACT.md from one lens: can a failure
in a helper (audit, heartbeat, respond, unlink, json.dumps of an odd result), a
permission or disk problem, or a leftover restart intent stop the agent from
staying alive and making progress? The fixture (temporary roots for every
filesystem path the bus knows, simulated os._exit) is the one used by
tests/test_contracts.py; that module is imported, not changed.

Tests that fail against the current code are confirmed breaks and are left in
place for the fix. Each docstring names the guarantee and the observed failure.
"""
import json
import os
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import comms          # noqa: E402
import agent_base     # noqa: E402
from tests.test_contracts import BusTestCase, SimulatedExit  # noqa: E402

NOT_ROOT = os.geteuid() != 0


class UnprintableError(Exception):
    """An exception whose __str__ is broken: a common bug in subclass code
    (for example a __str__ that reads an attribute the constructor did not set)."""

    def __str__(self):
        raise AttributeError("no message available")


class LivenessTestCase(BusTestCase):
    def setUp(self):
        super().setUp()
        # Wall-clock dream windows must not add alerts or learnings to these runs.
        self.agent.dream_times_utc = ()

    def alerts(self, kind):
        return sorted(p.name for p in (self.tmp / "to_main").glob(f"*{kind}*"))

    def make_dir_read_only(self, path):
        os.chmod(path, 0o555)
        self.addCleanup(os.chmod, path, 0o755)


class RestartIntentAtBoot(LivenessTestCase):
    """Guarantee 20: on the next boot an outstanding intent is reconciled, never
    re-executed. The reconciliation runs unguarded inside __init__, so a problem
    with the intent file or the state directory turns a restart into a boot
    failure, which under a supervisor is a crash loop."""

    def test_boot_survives_non_dict_restart_intent(self):
        """A restart_intent.json that decodes but is not an object (here the JSON
        string "accepted") raises AttributeError out of __init__: the agent
        cannot start at all. Undecodable content is already tolerated
        (restart_intent_corrupt); valid-but-wrong JSON must be too."""
        intent = self.agent.self_dir / "state" / "restart_intent.json"
        intent.write_text(json.dumps("accepted"))
        self.release(self.agent)
        try:
            self.agent = self.make_agent()
        except Exception as e:  # noqa: BLE001 - the whole point is that boot must not raise
            self.fail(f"__init__ raised on a malformed restart intent: {type(e).__name__}: {e}")
        self.assertTrue(self.events("restart_intent_corrupt") or self.events("restart_intent_reconciled"),
                        "the bad intent must be reported, not ignored")
        self.agent.tick()
        self.exit.assert_not_called()

    @unittest.skipUnless(NOT_ROOT, "permission bits do not apply to root")
    def test_boot_survives_unwritable_state_dir_when_intent_is_pending(self):
        """state/ is read-only (permissions clobbered, volume remounted) and a
        respawn intent in `accepted` is left from the previous process. Every
        other write in __init__ is guarded; the reconciliation write is not, so
        the PermissionError escapes and the daemon that was asked to restart
        can never come back up. Expected: boot, audit the failure, keep the
        intent for a later attempt, heartbeat on the first tick."""
        state = self.agent.self_dir / "state"
        (state / "restart_intent.json").write_text(json.dumps(
            {"event_id": "r1", "requested_by": "client", "status": "accepted"}))
        self.release(self.agent)
        self.make_dir_read_only(state)
        try:
            self.agent = self.make_agent()
        except Exception as e:  # noqa: BLE001
            self.fail(f"__init__ raised because state/ is read-only: {type(e).__name__}: {e}")
        self.agent.tick()
        self.heartbeat.assert_called_once_with("worker")
        self.exit.assert_not_called()

    def test_restart_intent_carries_the_transport_event_id(self):
        """Guarantee 3 allows a request without an envelope event_id (the
        filename stem is the id). healing_handler copies req["event_id"] into
        the intent, so such a respawn records event_id null and the boot-time
        `restart_intent_reconciled` audit cannot be correlated with the request
        that caused the restart."""
        (self.inbox() / "030.json").write_text(json.dumps(
            {"kind": "heal", "action": "respawn", "caller": "client"}))
        with self.assertRaises(SimulatedExit):
            self.agent.tick()
        intent = json.loads((self.agent.self_dir / "state" / "restart_intent.json").read_text())
        self.assertEqual(intent["event_id"], "030")
        (accepted,) = self.events("self_heal_respawn_accepted")
        self.assertEqual(accepted["eid"], "030")


class AuditCannotRaise(LivenessTestCase):
    """Guarantees 4, 7 and 24: audit() and _audit_safe() never raise, and any
    Exception from a handler is contained by _dispatch. Both helpers format the
    record outside their own try blocks."""

    def test_audit_safe_never_raises_on_unprintable_exception(self):
        """_audit_safe does str(exc) before entering its try; an exception whose
        __str__ raises escapes from the one helper that is documented as unable
        to raise. Every `except Exception as e: self._audit_safe(..., exc=e)`
        site in tick() and run() inherits the hole."""
        try:
            self.agent._audit_safe("probe", eid="x", exc=UnprintableError())
        except Exception as e:  # noqa: BLE001
            self.fail(f"_audit_safe raised: {type(e).__name__}: {e}")
        (rec,) = self.events("probe")
        self.assertEqual(rec["exception_type"], "UnprintableError")

    def test_audit_helpers_never_raise_on_unserializable_fields(self):
        """comms.audit builds its JSON line before the try, so a field that
        json.dumps cannot encode even with default=str (a circular structure,
        or a dict with tuple keys) raises to the caller. _audit_safe catches
        that, then rebuilds the same line outside its inner try and raises
        again. A subclass passing a stats object to its own audit call must not
        be able to bring the loop down."""
        circular = {}
        circular["self"] = circular
        try:
            comms.audit("worker", "probe_circular", data=circular)
        except Exception as e:  # noqa: BLE001
            self.fail(f"comms.audit raised: {type(e).__name__}: {e}")
        try:
            self.agent._audit_safe("probe_keys", per_kind={("work", 1): 2})
        except Exception as e:  # noqa: BLE001
            self.fail(f"_audit_safe raised: {type(e).__name__}: {e}")

    def test_dispatch_contains_exception_whose_str_raises(self):
        """The subclass handler raises UnprintableError. _dispatch's except
        clause calls _audit_safe(exc=e) and then formats str(e) for the reply;
        both raise, so the boundary of guarantee 7 leaks. Observed: the record is
        left in `executing`, the request stays in the inbox, and the next tick
        answers `uncertain` with an OUTCOME_UNCERTAIN alert to the operator,
        although nothing crashed. Expected: one `handler raised` failure reply,
        the request acknowledged, no uncertain alert."""
        request = self.queue()
        self.agent.raise_exc = UnprintableError()
        self.agent.tick()
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertFalse(request.exists(), "the request must be acknowledged after a contained failure")
        reply = self.response()
        self.assertIs(reply["ok"], False)
        self.assertIn("handler raised", reply.get("error", ""))
        self.assertEqual(self.alerts("OUTCOME_UNCERTAIN"), [], "a contained exception is not a crash window")
        self.assertEqual(self.events("outcome_uncertain"), [])
        self.assertTrue(self.events("handler_exception"))
        self.assertEqual(self.outcome()["status"], "acknowledged")


class OddResults(LivenessTestCase):
    """Guarantee 9: results are validated once and every downstream consumer
    uses the validated form. Guarantee 15: `uncertain` means the previous
    process stopped between running the handler and recording the outcome."""

    def test_unserializable_success_result_is_not_reported_as_uncertain(self):
        """The handler returns {"ok": True, "detail": {("a", "b"): 1}}: a dict
        with a boolean ok that passes _validate_result but that json.dumps
        rejects (tuple keys; default=str does not apply to keys). The `recorded`
        write fails, respond() fails on the same dumps, the batch stops and the
        record on disk is still `executing`. The next tick reads that as a crash
        window: the caller gets ok:false outcome:uncertain and the operator gets
        an OUTCOME_UNCERTAIN alert for a handler that completed normally.
        Expected: a reply with a JSON-boolean ok on the first tick (either the
        result or a schema failure), no uncertain outcome, handler run once."""
        request = self.queue()
        self.agent.result = {"ok": True, "detail": {("a", "b"): 1}}
        self.agent.tick()
        reply_path = comms.outbox_dir("client") / "001.json"
        self.assertTrue(reply_path.exists(), "a validated result must be publishable in the same tick")
        reply = json.loads(reply_path.read_text())
        self.assertIsInstance(reply["ok"], bool)
        self.assertNotEqual(reply.get("outcome"), "uncertain")
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertFalse(request.exists())
        self.assertEqual(self.alerts("OUTCOME_UNCERTAIN"), [])
        self.assertEqual(self.events("outcome_uncertain"), [])
        self.assertEqual(self.outcome()["status"], "acknowledged")


class DeliveryAndAcknowledgement(LivenessTestCase):
    """Guarantees 12, 13 and 25: a reply or acknowledgement failure is retried
    from the record and per-message failures are isolated from the rest of the
    batch."""

    @unittest.skipUnless(NOT_ROOT, "permission bits do not apply to root")
    def test_undeliverable_caller_does_not_starve_other_callers(self):
        """One caller's outbox is not writable (its directory exists with mode
        0o555). respond() fails for that message every tick, _process_message
        returns "stop", and because the inbox is drained in filename order the
        poisoned request is met first on every tick: no later message from any
        other caller ever runs, attempts.deliver grows without bound and the
        storage failures trip the breaker after five ticks, pausing idle work
        too. A per-destination failure is not a state-volume failure; the rest
        of the batch must proceed."""
        bad_outbox = comms.outbox_dir("badcaller")
        self.make_dir_read_only(bad_outbox)
        poisoned = self.queue(eid="000", caller="badcaller")
        healthy = self.queue(eid="001", caller="client")
        self.agent.tick()
        self.agent.tick()
        self.assertTrue(poisoned.exists(), "the undeliverable request itself stays pending")
        self.assertFalse(healthy.exists(), "a message for a different caller must not be starved")
        self.assertIs(self.response("001")["ok"], True)
        self.assertEqual([c["event_id"] for c in self.agent.calls], ["000", "001"])
        self.assertLess(self.agent._consec_failures, self.agent._circuit_breaker_threshold)

    def test_request_removed_before_ack_is_treated_as_acknowledged(self):
        """The request file disappears between dispatch and acknowledgement
        (an operator moving a stuck message out of the inbox by hand while the
        handler is running, or a second consumer on a platform without fcntl).
        path.unlink() raises FileNotFoundError, which is classified as a storage
        failure: the record is stranded at `delivered` (the next tick never sees
        the file again, so the retry of guarantee 13 never happens), the breaker
        counter is incremented, and the batch stops so the following message
        waits a full cadence. A missing file is an acknowledged file."""
        target = self.queue(eid="000")
        follower = self.queue(eid="001")

        def handle_and_vanish(req):
            if req["event_id"] == "000":
                target.unlink()
            return {"ok": True}
        self.agent.handle = handle_and_vanish
        self.agent.tick()
        self.assertEqual(self.outcome("000")["status"], "acknowledged")
        self.assertEqual(self.agent._consec_failures, 0)
        self.assertFalse(follower.exists(), "the batch must not stop because a file is already gone")
        self.assertIsNotNone(self.agent._last_progress)


# ---------------------------------------------------------------------------
# Round 2 — liveness lens: helper exceptions, permission errors at boot and in
# flight, restart-intent ordering, maintenance state that cannot be parsed.
# ---------------------------------------------------------------------------

class UnprintableValue:
    """A value a handler may legitimately put in its result (a stats object, a
    wrapped exception) whose __str__ is broken. json.dumps(default=str) calls
    str() on it and the AttributeError comes back out of json.dumps."""

    def __str__(self):
        raise AttributeError("no message available")


class ResultSerialisationBoundary(LivenessTestCase):
    """Guarantee 9: a result json.dumps(default=str) cannot encode is answered
    `ok: false` in the same tick and is never left as a crash window."""

    def test_result_value_whose_str_raises_is_answered_in_the_same_tick(self):
        """The handler returns {"ok": True, "detail": UnprintableValue()}.
        _validate_result only catches (TypeError, ValueError) around json.dumps;
        default=str raises AttributeError, which escapes _validate_result and
        _process_message. Observed: audit `message_exception`, the record stays
        `executing`, the request stays in the inbox, and the next tick answers
        `uncertain` with an OUTCOME_UNCERTAIN alert for a handler that returned
        normally. Expected: a reply with a JSON-boolean ok on the first tick,
        handler run once, record acknowledged, no uncertain outcome."""
        request = self.queue()
        self.agent.result = {"ok": True, "detail": UnprintableValue()}
        self.agent.tick()
        reply_path = comms.outbox_dir("client") / "001.json"
        self.assertTrue(reply_path.exists(), "an unencodable result must still be answered in the same tick")
        self.assertIsInstance(json.loads(reply_path.read_text())["ok"], bool)
        self.assertEqual(self.events("message_exception"), [])
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertFalse(request.exists())
        self.assertEqual(self.alerts("OUTCOME_UNCERTAIN"), [])
        self.assertEqual(self.outcome()["status"], "acknowledged")


class DreamSlotsCorruption(LivenessTestCase):
    """Guarantee 27: completed slots persist in state/dream_slots.json. An
    undecodable file is tolerated (reset to {}); a file that decodes to the
    wrong shape must be tolerated too, or the twice-daily self-doctor is gone."""

    def test_wrong_shaped_dream_slot_entry_does_not_disable_dreaming(self):
        """state/dream_slots.json holds {"2026-09-24T06:30": "completed"} (a
        hand edit, or an older writer that stored the status string directly).
        _maybe_dream does slot["status"] on the string and raises TypeError on
        every tick (audited as dream_check_exception, which never heals), so
        the 18:30 slot of the same day, and every later slot, never runs.
        Expected: the bad entry is reported and skipped or rewritten, and the
        due slot still runs."""
        from datetime import datetime, timezone, time as dtime
        self.agent.dream_times_utc = (dtime(6, 30), dtime(18, 30))
        slots_path = self.agent.self_dir / "state" / "dream_slots.json"
        slots_path.write_text(json.dumps({"2026-09-24T06:30": "completed"}))
        ran = []
        self.agent.dream_sequence = lambda: ran.append(1)
        try:
            self.agent._maybe_dream(now_utc=datetime(2026, 9, 24, 18, 31, tzinfo=timezone.utc))
        except Exception as e:  # noqa: BLE001
            self.fail(f"_maybe_dream raised on a wrong-shaped slot entry: {type(e).__name__}: {e}")
        self.assertEqual(len(ran), 1, "the due 18:30 slot must still run")
        slots = json.loads(slots_path.read_text())
        self.assertEqual(slots.get("2026-09-24T18:30", {}).get("status"), "completed")


class BootUnderPermissionErrors(LivenessTestCase):
    """Guarantees 16 and 20: boot survives a read-only state/ (audited, intent
    kept) and, where the single-consumer lock cannot be used, the agent reports
    consumer_lock_unavailable rather than dying. A boot failure under a
    supervisor is a restart loop with no daemon ever serving."""

    @unittest.skipUnless(NOT_ROOT, "permission bits do not apply to root")
    def test_boot_survives_read_only_state_dir_without_a_lock_file(self):
        """state/ is read-only and state/consumer.lock does not exist yet (a
        fresh deploy onto a clobbered volume, or an operator who removed the
        lock file while fixing permissions). Every other write in __init__ is
        guarded; lock_path.open("a+") is not, so PermissionError escapes
        __init__. The existing read-only-state test passes only because the
        lock file already exists. Expected: boot, audit the lock problem,
        heartbeat on the first tick."""
        state = self.agent.self_dir / "state"
        self.release(self.agent)
        (state / "consumer.lock").unlink()
        self.make_dir_read_only(state)
        try:
            self.agent = self.make_agent()
        except Exception as e:  # noqa: BLE001
            self.fail(f"__init__ raised because state/ is read-only: {type(e).__name__}: {e}")
        self.agent.tick()
        self.heartbeat.assert_called_once_with("worker")
        self.assertTrue(self.events("consumer_lock_unavailable") or self.events("consumer_lock_failed"),
                        "an unusable lock must be reported, not fatal")

    def test_flock_unsupported_on_the_volume_is_reported_not_misdiagnosed(self):
        """fcntl.flock fails with ENOLCK (locking unsupported on this volume:
        some network and synced filesystems) rather than EWOULDBLOCK. Every
        OSError is turned into RuntimeError("another consumer already holds
        ..."), so the operator is told a second daemon is running when none is,
        and the daemon never boots on that volume. Contract 16 defines exactly
        two outcomes: a real contender fails with RuntimeError; a platform
        without locking audits consumer_lock_unavailable and runs. ENOLCK is
        the second case."""
        import errno
        self.release(self.agent)

        def flock_unsupported(fd, op):
            raise OSError(errno.ENOLCK, "No locks available")
        with patch.object(agent_base.fcntl, "flock", flock_unsupported):
            try:
                self.agent = self.make_agent()
            except RuntimeError as e:
                self.fail(f"ENOLCK was reported as a second consumer: {e}")
        self.assertTrue(self.events("consumer_lock_unavailable"))


class RestartOrdering(LivenessTestCase):
    """Guarantee 20: os._exit(0) happens only once the respawn request's own
    record is `acknowledged`; another message reaching `acknowledged` while the
    respawn reply is still pending does not exit."""

    @unittest.skipUnless(NOT_ROOT, "permission bits do not apply to root")
    def test_rejected_identity_reuse_of_the_respawn_id_does_not_trigger_the_exit(self):
        """Tick 1: `heal: respawn` (eid 010) runs, the intent is `accepted`,
        the reply cannot be delivered (the caller's outbox is read-only), the
        record stays `recorded`, _pending_exit stays set. Tick 2: the outbox
        is writable again but the pending file was rewritten with different
        input under the same id. _process_message replies `rejection` using a
        transient record (persist=False), acknowledges the file, and then
        checks `pending.event_id == eid and rec.status == "acknowledged"` on
        that transient record: the daemon exits. The respawn's own record on
        disk is still `recorded`; its reply is never delivered (the file is
        gone) and the exit was triggered by acknowledging a rejected duplicate.
        Expected: no exit on this tick; the real record untouched."""
        outbox = comms.outbox_dir("client")
        self.make_dir_read_only(outbox)
        request = self.queue(eid="010", kind="heal", action="respawn")
        self.agent.tick()
        self.assertEqual(self.outcome("010")["status"], "recorded")
        self.assertTrue(self.events("response_delivery_failed"))
        os.chmod(outbox, 0o755)
        request.write_text(json.dumps({"event_id": "010", "kind": "heal", "action": "respawn",
                                       "caller": "client", "note": "altered"}))
        try:
            self.agent.tick()
        except SimulatedExit:
            self.fail("os._exit was called after acknowledging a rejected duplicate, "
                      "while the respawn's own record is still `recorded`")
        self.exit.assert_not_called()
        self.assertTrue(self.events("identity_reuse"))
        self.assertEqual(self.outcome("010")["status"], "recorded")


class InFlightPermissionErrors(LivenessTestCase):
    """Guarantees 3 and 23 and the monitoring table: filesystem failures are
    audited and retried, health.json tells the truth, and current_op.json
    exists only while a handler is running."""

    @unittest.skipUnless(NOT_ROOT, "permission bits do not apply to root")
    def test_unlistable_inbox_is_audited_not_reported_as_empty(self):
        """The inbox directory loses its read bit (mode 0o000) with a request
        inside. Path.glob swallows the PermissionError from scandir, so
        drain_inbox yields nothing, no audit event mentions the inbox, and
        health.json reports inbox_pending: 0 and alive: true. The agent has
        silently stopped consuming and every monitor signal says it is fine.
        A read OSError on one file is audited (inbox_read_error) and retried;
        a listing failure must be at least as visible. Expected: an audit
        event naming the inbox problem this tick, and inbox_pending not 0."""
        request = self.queue()
        inbox = self.inbox()
        os.chmod(inbox, 0o000)
        self.addCleanup(os.chmod, inbox, 0o755)
        self.agent.tick()
        os.chmod(inbox, 0o755)
        self.assertTrue(request.exists())
        self.heartbeat.assert_called_once_with("worker")
        inbox_events = [e["event"] for e in self.events() if "inbox" in e["event"]]
        self.assertTrue(inbox_events, "an inbox that cannot be listed must be audited")
        self.assertNotEqual(self.health()["inbox_pending"], 0,
                            "health must not report an unlistable inbox as empty")

    @unittest.skipUnless(NOT_ROOT, "permission bits do not apply to root")
    def test_current_op_that_cannot_be_removed_is_not_left_silently(self):
        """The handler runs while state/ becomes read-only (the handler itself
        does it here; a permissions clobber during a long handler does the
        same). current_op.json was written before the handler started; the
        unlink in the finally fails and is swallowed. The file now says a
        handler has been running since `started` although the tick completed,
        and the monitor's 'operation overdue' rule fires for an idle daemon
        until the next boot. Nothing is audited. Expected: either the file is
        gone after the tick, or an audit event records that it could not be
        removed (the stale-clear at boot audits exactly that case)."""
        state = self.agent.self_dir / "state"
        self.queue()

        def lock_state_then_succeed(req):
            os.chmod(state, 0o555)
            return {"ok": True}
        self.agent.handle = lock_state_then_succeed
        self.addCleanup(os.chmod, state, 0o755)
        self.agent.tick()
        os.chmod(state, 0o755)
        stale = (state / "current_op.json").exists()
        audited = [e for e in self.events() if e["event"].startswith("current_op")]
        self.assertTrue(not stale or audited,
                        "current_op.json outlived the handler with no audit of the failed removal")


# ---------------------------------------------------------------------------
# Round 3 — liveness lens: the audit-degraded signal, a non-regular operator
# key that blocks every tick, a helper exception that swallows the deferred
# exit, a silent current_op write failure, and a stored record that can never
# be replayed. Each test names the guarantee it pins and what was observed.
# ---------------------------------------------------------------------------

class AuditDegradedSignal(LivenessTestCase):
    """Guarantee 24 and the Monitoring table: every agent-side audit goes
    through _audit_safe, which "returns True when the primary audit call
    succeeded", and state/health.json carries `audit_degraded` so an operator
    can see that the audit trail is being diverted."""

    def test_audit_falling_back_is_visible_in_health_and_in_the_return_value(self):
        """AUDIT_DIR is unusable (a regular file sits where the directory
        should be, the same fault test_comms_audit_falls_back_and_never_raises
        uses). comms.audit() returns False and files the record under
        AGENTS_ROOT/_audit_fallback/, as designed. _audit_safe ignores that
        return value: it only flips _audit_degraded when audit() raises, which
        audit() is documented never to do. Observed: _audit_safe returns True,
        health.json says audit_degraded: false, and logs/audit_fallback.jsonl
        (the fallback guarantee 24 names) is never written, while every record
        of the tick is diverted. The one signal the contract gives an operator
        for a diverted audit trail can never fire."""
        blocker = self.tmp / "audit-blocker"
        blocker.write_text("not a directory")
        with patch.object(comms, "AUDIT_DIR", blocker):
            primary_ok = self.agent._audit_safe("probe_degraded", n=1)
            self.agent.tick()
            health = self.health()
        diverted = list((self.tmp / "agents" / "_audit_fallback").glob("*_worker.jsonl"))
        self.assertTrue(diverted, "precondition: the records were diverted to the bus fallback")
        self.assertFalse(primary_ok, "_audit_safe must not report success when the primary audit write failed")
        self.assertTrue(health["audit_degraded"],
                        "health.audit_degraded must be true in the tick whose audit records were diverted")


class OperatorKeyThatIsNotARegularFile(LivenessTestCase):
    """Guarantee 22: an unreadable key refuses privileged requests and health
    reports `privileged_auth_key: unreadable`. _operator_key() is called on
    every tick from _write_health, so whatever it does happens at least once
    per cadence for every agent on the bus."""

    def test_fifo_operator_key_does_not_block_the_tick(self):
        """AGENTS_ROOT/_trust/operator.key is a FIFO (an operator's mkfifo
        slip, or a provisioning tool that stages the key through a pipe).
        _operator_key does p.exists() then p.read_bytes(); open(2) on a FIFO
        with no writer blocks forever. read_message() guards the inbox against
        exactly this ("a FIFO would block the tick forever") and known_agents()
        checks is_file() before reading the registry; the key path does not.
        Observed: tick() never returns, so the heartbeat of guarantee 23 stops
        for every agent that shares the bus, and the monitor reports them dead.
        Expected: a non-regular key is `unreadable` (privileged requests are
        refused) and the tick completes."""
        trust = self.tmp / "agents" / "_trust"
        trust.mkdir()
        key = trust / "operator.key"
        os.mkfifo(key)
        finished = []
        worker = threading.Thread(target=lambda: (self.agent.tick(), finished.append(True)), daemon=True)
        worker.start()
        worker.join(2.0)
        if not finished:
            # Give the blocked reader an EOF so the thread can leave before the fixture is torn down.
            try:
                fd = os.open(key, os.O_WRONLY | os.O_NONBLOCK)
                os.close(fd)
            except OSError:
                pass
            worker.join(2.0)
            self.fail("tick() blocked on a FIFO at _trust/operator.key; the heartbeat never happened")
        self.heartbeat.assert_called_once_with("worker")
        self.assertEqual(self.health()["privileged_auth_key"], "unreadable")


class DeferredExitSurvivesHelperFailure(LivenessTestCase):
    """Guarantee 20: once the respawn request's own record is acknowledged the
    tick heartbeats, marks the intent `exiting` and calls os._exit(0). The exit
    is the last statement of tick(), after the heartbeat's finally block."""

    def test_heartbeat_exception_does_not_lose_the_accepted_restart(self):
        """The respawn request is recorded, replied to and acknowledged; the
        intent on disk is `accepted`. The heartbeat helper then raises (here
        the patched helper does it directly; in production any exception the
        helper's own guards miss). tick()'s finally re-raises after writing
        health, so `if exit_after: self._controlled_exit()` is never reached.
        Observed: no exit, _pending_exit stays set, health reports
        restart_pending: true on every later tick, and because the request was
        acknowledged no later tick can ever return "exit" for it: the accepted
        restart is lost until a second respawn arrives. Expected: the accepted
        restart is carried out, on this tick or once the heartbeat works again."""
        self.queue(eid="010", kind="heal", action="respawn")
        self.heartbeat.side_effect = RuntimeError("heartbeat helper failed")
        try:
            self.agent.tick()
        except SimulatedExit:
            pass
        except Exception:  # noqa: BLE001 - a raising helper must not decide the restart
            pass
        intent_path = self.agent.self_dir / "state" / "restart_intent.json"
        self.assertEqual(self.outcome("010")["status"], "acknowledged", "precondition: the respawn was acknowledged")
        if not self.exit.called:
            self.heartbeat.side_effect = comms.heartbeat
            try:
                self.agent.tick()
            except SimulatedExit:
                pass
        self.assertTrue(self.exit.called,
                        "an acknowledged respawn must exit; the accepted restart was dropped because the heartbeat helper raised")
        self.assertEqual(json.loads(intent_path.read_text())["status"], "exiting")


class CurrentOpWriteFailure(LivenessTestCase):
    """Monitoring table: `state/current_op.json` age against `deadline_s` is
    the only signal for a hung handler, and "the agent cannot report this
    itself". If the file cannot be written, that signal is disabled."""

    @unittest.skipUnless(NOT_ROOT, "permission bits do not apply to root")
    def test_current_op_that_cannot_be_written_is_audited(self):
        """state/ is read-only while state/outcomes/ is still writable (the
        directory bits differ: a permissions clobber on state/ alone, or an
        operator who fixed outcomes/ first). The executing claim lands, so the
        handler runs; the current_op.json write raises and is swallowed by a
        bare `except Exception: pass`. Nothing in the audit trail says the
        hung-handler signal was off for this message, although the matching
        removal failure is audited (`current_op_unremovable`, guarantee 20).
        Expected: the tick completes (it does) and an audit event records that
        current_op.json could not be written."""
        state = self.agent.self_dir / "state"
        self.queue()
        self.make_dir_read_only(state)
        self.agent.tick()
        os.chmod(state, 0o755)
        self.assertEqual(len(self.agent.calls), 1, "precondition: the handler ran")
        self.assertEqual(self.outcome()["status"], "acknowledged")
        audited = [e for e in self.events() if e["event"].startswith("current_op")]
        self.assertTrue(audited, "current_op.json could not be written and nothing was audited")


class RecordThatCannotBeReplayed(LivenessTestCase):
    """Guarantees 12, 14 and 15: a `recorded` record is redelivered from the
    record without running the handler; a record that cannot be read as an
    outcome is answered `uncertain` and acknowledged, never left to wedge a
    request. `respond` raises TypeError for a non-dict payload."""

    def test_recorded_record_with_a_non_object_result_is_not_retried_forever(self):
        """state/outcomes/001.json holds status `recorded`, the matching
        fingerprint, and `result: "done"` (a hand edit, or a record written
        by a different revision). _load_outcome accepts it (known status), the
        redelivery branch does `rec.get("result") or {...}`, keeps the truthy
        string and calls respond(caller, eid, "done"), which raises TypeError.
        That is treated as the caller's outbox failing: attempts.deliver is
        bumped, `response_delivery_failed` is audited, the file stays, and
        because the outcome is "retry" it is never charged to the budget.
        Observed over three ticks: three response_delivery_failed audits that
        blame a healthy outbox, no reply, the request still pending, forever.
        Expected: the request reaches a terminal state (a reply with a JSON
        boolean ok, the file acknowledged) within a tick or two."""
        import hashlib
        request = self.queue()
        fingerprint = hashlib.sha256(json.dumps(
            {"event_id": "001", "kind": "work", "caller": "client"},
            sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        (self.agent.self_dir / "state" / "outcomes" / "001.json").write_text(json.dumps({
            "event_id": "001", "status": "recorded", "fingerprint": fingerprint,
            "kind": "work", "caller": "client", "result": "done",
            "attempts": {"execute": 1, "deliver": 0, "ack": 0}}))
        self.agent.tick()
        self.agent.tick()
        self.assertEqual(self.agent.calls, [], "a recorded outcome must not run the handler again")
        self.assertFalse(request.exists(), "a record that cannot be replayed must not keep the request pending forever")
        reply_path = comms.outbox_dir("client") / "001.json"
        self.assertTrue(reply_path.exists(), "the caller must get a reply")
        self.assertIsInstance(json.loads(reply_path.read_text())["ok"], bool)
        self.assertEqual(self.events("response_delivery_failed"), [],
                         "a broken record must not be reported as the caller's outbox failing")


if __name__ == "__main__":
    unittest.main(verbosity=2)

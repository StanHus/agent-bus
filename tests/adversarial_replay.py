"""Adversarial tests, replay lens: idempotency across crash windows, duplicate
event ids, overlapping consumers, partial writes.

    /opt/homebrew/bin/python3 -m unittest tests.adversarial_replay -v

Reuses the temp-directory harness from tests/test_contracts.py; nothing here
touches the real agent tree. Each test names the guarantee in CONTRACT.md it
attacks. Tests that fail against the current code are confirmed breaks and are
left in place for the fix.
"""
import hashlib
import os
import json
import sys
from datetime import datetime, timezone, timedelta, time as dtime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import comms          # noqa: E402
import agent_base     # noqa: E402
from tests.test_contracts import BusTestCase, Worker, SimulatedExit  # noqa: E402


class UnlockedWorker(Worker):
    """A consumer on a platform without fcntl: CONTRACT.md guarantee 16 says the
    record-based protection of guarantee 15 is then all that stands between two
    consumers."""

    def _acquire_consumer_lock(self):
        self._consumer_lock = None


class ReplayAdversarial(BusTestCase):

    # -- guarantee 14: duplicates ------------------------------------------
    def test_duplicate_after_crash_between_ack_and_record_is_replayed(self):
        """Crash window: unlink(request) done, `acknowledged` record not yet
        written. The record stays `delivered`. Guarantee 14 says a same-input
        resend gets the recorded result republished and audited
        duplicate_replayed; the lifecycle table only replays from
        `acknowledged`, so the resend is silently acknowledged with no reply."""
        self.queue(payload=1)
        real_save = self.agent._save_outcome

        def die_before_ack_record(eid, rec):
            if rec.get("status") == "acknowledged":
                raise SimulatedExit("power loss after unlink, before the acknowledged record")
            return real_save(eid, rec)

        with patch.object(self.agent, "_save_outcome", side_effect=die_before_ack_record):
            with self.assertRaises(SimulatedExit):
                self.agent.tick()
        self.assertEqual(self.outcome()["status"], "delivered")
        self.assertFalse((self.inbox() / "001.json").exists())
        # The caller consumed the reply, then (client retry after its own restart) resent the same input.
        (comms.outbox_dir("client") / "001.json").unlink()
        self.release(self.agent)
        self.agent = self.make_agent()
        self.queue(payload=1)
        self.agent.tick()
        self.assertEqual(self.agent.calls, [], "handler must not run for a duplicate")
        self.assertEqual(list(self.inbox().glob("*.json")), [], "duplicate must be acknowledged")
        self.assertTrue((comms.outbox_dir("client") / "001.json").exists(),
                        "same-input duplicate was acknowledged without republishing the recorded result")
        self.assertEqual(self.response()["value"], "done")
        self.assertTrue(self.events("duplicate_replayed"))

    # -- guarantee 15/16: overlapping consumers -----------------------------
    def test_executing_claim_is_not_exclusive_so_a_stale_reader_double_executes(self):
        """The `executing` record is written with tmp+rename, which overwrites.
        A second consumer that read the outcomes directory before the first
        consumer's claim landed (no record yet) writes its own claim on top and
        runs the handler too. Guarantee 16 says a message whose record says
        executing is never run by another consumer; the claim must be an
        exclusive create for that to hold."""
        self.queue()
        first = self.agent
        second = self.make_agent(UnlockedWorker)
        # What `second` saw when it read the outcome record a moment before `first` claimed: nothing.
        stale_snapshot = second._load_outcome("001")
        self.assertIsNone(stale_snapshot)
        second._load_outcome = lambda eid: stale_snapshot

        def first_handle(req):
            first.calls.append(req)
            second.tick()            # the other consumer arrives while first is inside the handler
            return {"ok": True, "value": "done"}

        first.handle = first_handle
        first.tick()
        total = len(first.calls) + len(second.calls)
        self.assertEqual(total, 1, f"one event id, handler executed {total} times across two consumers")

    # -- guarantee 25 / REVIEW.md section 2: per-message isolation ----------
    def test_one_callers_unwritable_outbox_does_not_block_other_callers(self):
        """A reply failure is `storage` and returns 'stop', which ends the batch.
        When the failure is one caller's outbox (permissions, quota, a caller
        whose directory was removed), every message from every other caller
        behind it is never reached, on every tick, and the storage count trips
        the agent-wide breaker after five ticks. Guarantee 25 promises per-
        message failures are isolated and the following messages still run."""
        self.queue(eid="000", caller="broken")
        self.queue(eid="001", caller="healthy")
        real_respond = comms.respond

        def respond(caller, eid, payload):
            if caller == "broken":
                raise PermissionError("outbox for 'broken' is not writable")
            return real_respond(caller, eid, payload)

        with patch.object(agent_base, "respond", side_effect=respond):
            self.agent.tick()
        self.assertEqual([r["event_id"] for r in self.agent.calls], ["000", "001"],
                         "a reply failure for one caller stopped the batch before the next caller's message")
        self.assertIs(self.response("001", "healthy")["ok"], True)
        self.assertFalse((self.inbox() / "001.json").exists())
        self.assertEqual(self.outcome("000")["status"], "recorded", "the broken caller's work is kept, not repeated")

    # -- guarantee 20: respawn ordering -------------------------------------
    def test_exit_waits_for_the_respawn_request_itself_to_be_acknowledged(self):
        """`_pending_exit` is set inside the handler; the exit is triggered by
        the first message that reaches `acknowledged` afterwards, whichever
        message that is. If the respawn reply could not be written (storage),
        an unrelated later message's acknowledgement exits the process while
        the respawn request is still in the inbox in `recorded`. Guarantee 20:
        records, replies, acknowledges, heartbeats, then exits."""
        respawn = self.queue(eid="002", kind="heal", action="respawn")
        real_respond = comms.respond

        def respond(caller, eid, payload):
            if eid == "002":
                raise OSError("caller outbox unavailable")
            return real_respond(caller, eid, payload)

        with patch.object(agent_base, "respond", side_effect=respond):
            self.agent.tick()
            self.assertEqual(self.outcome("002")["status"], "recorded")
            self.exit.assert_not_called()
            self.queue(eid="001")     # ordinary work; sorts before the respawn
            exited = False
            try:
                self.agent.tick()
            except SimulatedExit:
                exited = True
        if exited:
            self.assertFalse(respawn.exists(),
                             "process exited on another message's ack while the respawn request was unacknowledged")
            self.assertEqual(self.outcome("002")["status"], "acknowledged")

    # -- monitoring signal after a hard crash --------------------------------
    def test_current_op_left_by_a_hard_crash_is_cleared_on_boot(self):
        """state/current_op.json 'exists only while a handler is running'. A
        SIGKILL or power loss mid-handler leaves it behind (the finally never
        runs). The next boot does not remove it, so the 'operation overdue'
        signal fires for a healthy idle process until the next message happens
        to be handled. Boot should reconcile it like restart_intent.json."""
        op = self.agent.self_dir / "state" / "current_op.json"
        self.release(self.agent)
        op.write_text(json.dumps({"event_id": "dead", "kind": "work", "caller": "client",
                                  "started": "2026-01-01T00:00:00Z", "deadline_s": 30}))
        self.agent = self.make_agent()
        self.agent.tick()
        self.assertFalse(op.exists(), "stale current_op.json from a crashed process survives boot and a tick")
        self.assertIsNone(self.health()["current_op"])

    # -- probes that hold (kept so the coverage is visible) -----------------
    def test_truncated_outcome_record_is_uncertain_not_rerun(self):
        """Partial write of the durable record (half a JSON object, or zero
        bytes) must read as `corrupt` and answer uncertain, never re-execute;
        a same-input resend of that message replays the uncertain result."""
        for eid, body in (("001", '{"event_id": "001", "status": "exec'), ("002", "")):
            self.queue(eid=eid)
            (self.agent.self_dir / "state" / "outcomes" / f"{eid}.json").write_text(body)
            self.agent.tick()
            self.assertEqual(self.agent.calls, [])
            self.assertEqual(self.response(eid)["outcome"], "uncertain")
            self.assertEqual(self.outcome(eid)["status"], "acknowledged")
            self.queue(eid=eid)
            self.agent.tick()
            self.assertEqual(self.agent.calls, [])
            self.assertEqual(list(self.inbox().glob("*.json")), [])

    def test_idempotent_kind_reexecution_has_no_attempt_bound(self):
        """Documented, not a contract break: a kind in IDEMPOTENT_KINDS whose
        handler kills the process every time is re-executed on every restart
        with no cap on attempts.execute. This test records the behaviour."""
        self.agent.IDEMPOTENT_KINDS = ("work",)
        self.queue()
        rec_path = self.agent.self_dir / "state" / "outcomes" / "001.json"
        fingerprint = hashlib.sha256(json.dumps({"event_id": "001", "kind": "work", "caller": "client"},
                                                sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        rec_path.write_text(json.dumps({"event_id": "001", "status": "executing", "kind": "work",
                                        "caller": "client", "attempts": {"execute": 500, "deliver": 0, "ack": 0},
                                        "fingerprint": fingerprint}))
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(self.outcome()["attempts"]["execute"], 501)


class SmallBudgetWorker(Worker):
    """Per-tick message budget of two, so a starvation pattern needs three files."""
    max_messages_per_tick = 2


class ReplayAdversarialRound2(BusTestCase):
    """Round 2 of the replay lens. Same harness; each test names the guarantee
    it attacks. Failing tests are confirmed breaks left in place for the fix."""

    def record_path(self, eid="001"):
        return self.agent.self_dir / "state" / "outcomes" / f"{eid}.json"

    # -- guarantee 15: idempotent re-execution after a partial record ----------
    def test_idempotent_kind_with_corrupt_record_is_reexecuted_not_stuck(self):
        """Guarantee 15: a record found executing *or unreadable* re-executes when
        the kind is in IDEMPOTENT_KINDS. A truncated record loads as
        {'status': 'corrupt'} without `attempts`, so the re-execution write is
        treated as a first claim (exclusive create) and hits the existing file:
        claim_conflict is audited, the handler never runs and the request is
        never acknowledged, on every tick, forever."""
        self.agent.IDEMPOTENT_KINDS = ("work",)
        request = self.queue()
        self.record_path().write_text('{"event_id": "001", "status": "exec')
        for _ in range(3):
            self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1,
                         "idempotent kind with a corrupt record must be re-executed once")
        self.assertFalse(request.exists(), "re-executed idempotent request must be acknowledged")
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(self.events("claim_conflict"), [],
                         "a corrupt record is not another consumer's claim")

    def test_non_object_outcome_record_is_uncertain_not_stuck_or_rejected(self):
        """Guarantee 15: an unreadable record means uncertain. A record that is
        valid JSON but not the documented object (null, [], {}) is 'read' as
        None, a list or an empty dict: `null` makes the message look new and the
        exclusive claim then fails forever (claim_conflict every tick), `[]`
        raises inside _process_message every tick (message_exception), and `{}`
        has no fingerprint so the request is answered as identity_reuse and
        acknowledged without ever running or being marked uncertain."""
        for eid, body in (("001", "null"), ("002", "[]"), ("003", "{}")):
            with self.subTest(record=body):
                request = self.queue(eid=eid)
                self.record_path(eid).write_text(body)
                self.agent.tick()
                self.assertEqual(self.agent.calls, [], "handler must not run against an unreadable record")
                self.assertFalse(request.exists(), f"request with record {body!r} was never acknowledged")
                self.assertEqual(self.response(eid).get("outcome"), "uncertain",
                                 f"record {body!r} must be answered uncertain, not {self.response(eid)}")
                self.assertEqual(self.outcome(eid)["status"], "acknowledged")

    # -- guarantee 15/16: a live claim is not a crash window -------------------
    def test_live_claim_by_another_consumer_is_not_treated_as_a_crash_window(self):
        """Without fcntl (guarantee 16) the executing check of guarantee 15 is the
        only protection. A second consumer that loads the record while the first
        is inside the handler reads `executing` and concludes 'the previous
        process stopped': it replies uncertain, raises OUTCOME_UNCERTAIN to the
        operator and acknowledges (unlinks) a request another consumer is still
        executing. The caller then receives two contradictory replies for one
        event id. The record carries no owner or lease, so nothing in it lets
        the second consumer tell a live claim from a dead one."""
        self.queue()
        first = self.agent
        second = self.make_agent(UnlockedWorker)
        seen_by_caller = []

        def first_handle(req):
            first.calls.append(req)
            second.tick()      # the other consumer arrives while first is inside the handler
            reply = comms.outbox_dir("client") / "001.json"
            if reply.exists():
                seen_by_caller.append(json.loads(reply.read_text()))
            return {"ok": True, "value": "done"}

        first.handle = first_handle
        first.tick()
        self.assertEqual(len(first.calls) + len(second.calls), 1)
        self.assertEqual(self.events("outcome_uncertain"), [],
                         "a record being executed by a live consumer was reported as a crash window")
        self.assertEqual(seen_by_caller, [],
                         f"caller received a reply while the handler was still running: {seen_by_caller}")
        self.assertEqual(self.events("ack_already_gone"), [],
                         "the second consumer acknowledged a request the first was still executing")

    # -- guarantee 10/12/26: delivery retries must not starve the batch --------
    def test_delivery_retries_do_not_consume_the_whole_tick_budget(self):
        """Guarantee 10: one caller's unwritable outbox 'does not starve the
        others'. A delivery retry counts toward max_messages_per_tick and the
        undeliverable requests stay in the inbox, sorted first. Once a caller has
        as many stuck requests as the budget, every tick spends the whole budget
        redelivering to that caller, audits tick_budget_exhausted and never
        reaches the next caller's message."""
        self.release(self.agent)
        self.agent = self.make_agent(SmallBudgetWorker)
        self.queue(eid="000", caller="broken")
        self.queue(eid="001", caller="broken")
        healthy = self.queue(eid="002", caller="healthy")
        real_respond = comms.respond

        def respond(caller, eid, payload):
            if caller == "broken":
                raise PermissionError("outbox for 'broken' is not writable")
            return real_respond(caller, eid, payload)

        with patch.object(agent_base, "respond", side_effect=respond):
            for _ in range(4):
                self.agent.tick()
        self.assertIn("002", [r["event_id"] for r in self.agent.calls],
                      "the healthy caller's request was never reached behind two undeliverable ones")
        self.assertFalse(healthy.exists())
        self.assertEqual(self.outcome("000")["status"], "recorded", "the broken caller's work is kept, not repeated")

    # -- guarantee 27: dream retry bound across crashes ------------------------
    def test_dream_attempt_is_durable_before_the_run_so_a_crashing_dream_is_bounded(self):
        """Guarantee 27: 'a failed slot retries at most three times' and a
        restart inside the window does not repeat a slot. The attempt count is
        written only after dream_sequence returns, so a dream that takes the
        process down (OOM, SIGKILL, os._exit) leaves the slot pending with
        attempts 0 on disk and is run again on every restart, with catch-up for
        the rest of the UTC day: a crash loop with no bound."""
        now = datetime(2026, 1, 1, 6, 31, tzinfo=timezone.utc)
        runs = 0
        for boot in range(5):
            self.release(self.agent)
            self.agent = self.make_agent()
            self.agent.dream_times_utc = (dtime(6, 30),)
            def dying_dream():
                nonlocal runs
                runs += 1
                raise SimulatedExit("dream took the process down")
            self.agent.dream_sequence = dying_dream
            try:
                self.agent._maybe_dream(now_utc=now + timedelta(minutes=boot))
            except SimulatedExit:
                pass
        self.assertLessEqual(runs, 3, f"a dream that crashes the process was run {runs} times across 5 boots")
        slots = json.loads((self.agent.self_dir / "state" / "dream_slots.json").read_text())
        self.assertEqual(slots["2026-01-01T06:30"]["attempts"], 3)

    # -- guarantee 14: canonical input, not canonical envelope -----------------
    def test_duplicate_that_differs_only_in_transport_defaults_is_replayed_not_rejected(self):
        """Guarantee 7 gives the handler `event_id` from the filename and `caller`
        defaulted, so two envelopes that differ only in whether they spell those
        out are the same input. Guarantee 14 fingerprints the raw envelope: a
        resend that adds the explicit `event_id`, or omits it, is answered as
        identity_reuse (a rejection) instead of replaying the recorded result."""
        path = self.inbox() / "001.json"
        path.write_text(json.dumps({"kind": "work", "caller": "client"}))   # envelope omits event_id
        self.agent.tick()
        self.assertEqual(self.response()["value"], "done")
        (comms.outbox_dir("client") / "001.json").unlink()
        self.queue()   # same request, this time with the explicit event_id the handler saw anyway
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(self.events("identity_reuse"), [],
                         "a same-input resend spelled with the transport event_id was rejected as identity reuse")
        self.assertEqual(self.response().get("value"), "done")
        self.assertTrue(self.events("duplicate_replayed"))

    # -- probes that hold (coverage made visible) -----------------------------
    def test_crash_between_reply_and_delivered_record_redelivers_without_rerun(self):
        """Guarantee 12: the reply is out, the process dies before `delivered`
        is written. Next tick: redeliver from the record, acknowledge, no handler."""
        self.queue()
        real_save = self.agent._save_outcome

        def die_before_delivered(eid, rec):
            if rec.get("status") == "delivered":
                raise SimulatedExit("power loss after respond, before the delivered record")
            return real_save(eid, rec)

        with patch.object(self.agent, "_save_outcome", side_effect=die_before_delivered):
            with self.assertRaises(SimulatedExit):
                self.agent.tick()
        self.assertEqual(self.outcome()["status"], "recorded")
        self.assertTrue((comms.outbox_dir("client") / "001.json").exists())
        self.release(self.agent)
        self.agent = self.make_agent()
        self.agent.tick()
        self.assertEqual(self.agent.calls, [])
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(list(self.inbox().glob("*.json")), [])
        self.assertEqual(self.response()["value"], "done")

    def test_crash_between_recorded_and_reply_delivers_without_rerun(self):
        """Guarantee 11/12: the outcome is recorded, the process dies before
        respond. Next tick delivers the recorded result and acknowledges."""
        self.queue()
        with patch.object(agent_base, "respond", side_effect=SimulatedExit("died before respond")):
            with self.assertRaises(SimulatedExit):
                self.agent.tick()
        self.assertEqual(self.outcome()["status"], "recorded")
        self.release(self.agent)
        self.agent = self.make_agent()
        self.agent.tick()
        self.assertEqual(self.agent.calls, [])
        self.assertEqual(self.response()["value"], "done")
        self.assertEqual(self.outcome()["status"], "acknowledged")

    def test_leftover_claim_tmp_from_a_dead_process_does_not_block_the_claim(self):
        """Guarantee 11: the claim writes a private tmp then hard-links it. A
        crash between the two leaves `<eid>.json.<pid>.tmp` behind; it must not
        make the next process believe the message is claimed."""
        self.queue()
        stale = self.agent.self_dir / "state" / "outcomes" / f"001.json.{os.getpid()}.tmp"
        stale.write_text('{"event_id": "001", "status": "executing"}')
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(self.events("claim_conflict"), [])

    def test_recorded_write_failure_then_resend_does_not_rerun_non_idempotent_kind(self):
        """Guarantee 10/15: the `recorded` write fails (storage), the reply still
        goes out and the request is acknowledged while the durable record says
        executing. A same-input resend must be answered uncertain, never re-run."""
        self.queue()
        real_save = self.agent._save_outcome

        def fail_recorded(eid, rec):
            if rec.get("status") in ("recorded", "delivered", "acknowledged"):
                return False
            return real_save(eid, rec)

        with patch.object(self.agent, "_save_outcome", side_effect=fail_recorded):
            self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(self.outcome()["status"], "executing")
        self.assertEqual(list(self.inbox().glob("*.json")), [])
        self.queue()
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1, "handler re-ran after a storage failure")
        self.assertEqual(self.response()["outcome"], "uncertain")

    def test_respawn_resent_after_the_restart_replays_and_does_not_exit_again(self):
        """Guarantee 20: the respawn request's own id acknowledged then exit; on
        the next boot a same-id resend is a duplicate and must not restart again."""
        self.queue(eid="002", kind="heal", action="respawn")
        with self.assertRaises(SimulatedExit):
            self.agent.tick()
        self.assertEqual(self.outcome("002")["status"], "acknowledged")
        self.release(self.agent)
        self.agent = self.make_agent()
        self.exit.reset_mock()
        (comms.outbox_dir("client") / "002.json").unlink()
        self.queue(eid="002", kind="heal", action="respawn")
        self.agent.tick()
        self.exit.assert_not_called()
        self.assertEqual(self.response("002")["healed"], "respawn")
        self.assertTrue(self.events("duplicate_replayed"))
        intent = json.loads((self.agent.self_dir / "state" / "restart_intent.json").read_text())
        self.assertEqual(intent["status"], "completed")

    def test_sender_rewrite_of_the_same_request_between_read_and_ack_is_harmless(self):
        """The sender's retry lands (write .tmp, rename over the same name) while
        the handler runs. Same fingerprint: the acknowledgement removes the
        resend and the recorded result stands; nothing runs twice."""
        request = self.queue(payload=1)
        body = request.read_text()

        def handle(req):
            self.agent.calls.append(req)
            tmp = request.with_name(request.name + ".tmp")
            tmp.write_text(body)
            tmp.replace(request)
            return {"ok": True, "value": "done"}

        self.agent.handle = handle
        self.agent.tick()
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertFalse(request.exists())
        self.assertEqual(self.outcome()["status"], "acknowledged")


class ReplayAdversarialRound3(BusTestCase):
    """Round 3 of the replay lens: the claim's no-hard-link fallback, replies
    that are present but foreign, an owner that outlives its lease, and the
    crash windows around respond(). Same harness; each test names the
    guarantee it attacks. Failing tests are confirmed breaks left in place."""

    def record_path(self, eid="001"):
        return self.agent.self_dir / "state" / "outcomes" / f"{eid}.json"

    # -- guarantee 11/15: a failed claim must not leave a record behind --------
    def test_failed_exclusive_create_claim_does_not_leave_a_record_that_answers_uncertain(self):
        """Guarantee 11: 'if the executing write fails the handler is not run
        and the batch stops with every file left in place'. On a filesystem
        without hard links the claim falls back to an exclusive open of the
        final record path and then writes the bytes through it. When that write
        fails (ENOSPC, EIO, a quota) the record file already exists, empty or
        partial. The next tick reads it as `corrupt`, answers the caller
        `uncertain` and never runs the handler: a transient storage error on
        the claim turned into a lost message, although nothing ever executed.
        The hard-link path (private tmp, then link) has no such window."""
        import errno as _errno
        request = self.queue()
        final = self.record_path()

        class BrokenHandle:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def write(self, data):
                raise OSError(_errno.ENOSPC, "No space left on device")

        def fdopen_that_cannot_write(fd, *a, **k):
            os.close(fd)
            return BrokenHandle()

        no_links = PermissionError(_errno.EPERM, "hard links are not supported on this volume")
        with patch.object(os, "link", side_effect=no_links), \
                patch.object(os, "fdopen", side_effect=fdopen_that_cannot_write):
            self.agent.tick()
        # The failed claim: handler not run, batch stopped, request kept, failure audited.
        self.assertEqual(self.agent.calls, [], "handler must not run when the claim cannot be written")
        self.assertTrue(request.exists(), "the request must stay in the inbox after a failed claim")
        self.assertTrue(self.events("outcome_record_failed"))
        self.assertFalse(final.exists(),
                         "a claim whose write failed left a record file behind (its bytes are not the record)")
        # Storage recovered: the next tick must run the handler exactly once and answer with its result.
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1,
                         "after a failed claim the retry must execute the handler, not answer uncertain")
        self.assertEqual(self.response().get("value"), "done", self.response())
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(self.events("outcome_uncertain"), [],
                         "a message that never executed was reported as a crash window")

    # -- guarantee 14: 'reply still present' must mean the recorded reply ------
    def test_same_input_duplicate_after_lost_ack_record_is_replayed_even_when_a_foreign_reply_occupies_the_slot(self):
        """Guarantee 14: from `delivered`, a same-input resend replays the
        recorded result when the reply is no longer in the caller's outbox.
        The check is only 'does <caller>/outbox/<eid>.json exist', not 'is it
        the recorded reply'. Sequence: the process dies between the unlink and
        the `acknowledged` write (record stays `delivered`); the caller
        consumes the reply; the sender misuses the id once with different
        input and gets an identity_reuse rejection written into that same
        outbox slot; the sender then resends the original input. The record is
        `delivered`, a file named <eid>.json is present, so the resend is only
        acknowledged: the recorded result is never republished and the caller
        is left holding the rejection of a request it did not make."""
        self.queue(payload=1)
        real_save = self.agent._save_outcome

        def die_before_ack_record(eid, rec):
            if rec.get("status") == "acknowledged":
                raise SimulatedExit("power loss after unlink, before the acknowledged record")
            return real_save(eid, rec)

        with patch.object(self.agent, "_save_outcome", side_effect=die_before_ack_record):
            with self.assertRaises(SimulatedExit):
                self.agent.tick()
        self.assertEqual(self.outcome()["status"], "delivered")
        reply = comms.outbox_dir("client") / "001.json"
        self.assertEqual(json.loads(reply.read_text())["value"], "done")
        reply.unlink()                                   # the caller consumed the reply
        self.release(self.agent)
        self.agent = self.make_agent()
        self.queue(payload=2)                            # sender misuse: same id, different input
        self.agent.tick()
        self.assertEqual(self.agent.calls, [])
        self.assertTrue(self.events("identity_reuse"))
        self.assertEqual(json.loads(reply.read_text())["failure_class"], "rejection")
        self.assertEqual(self.outcome()["status"], "delivered", "the original record must stay untouched")
        self.queue(payload=1)                            # the original input, resent
        self.agent.tick()
        self.assertEqual(self.agent.calls, [], "handler must not run for a duplicate")
        self.assertEqual(list(self.inbox().glob("*.json")), [], "duplicate must be acknowledged")
        self.assertTrue(self.events("duplicate_replayed"),
                        "same-input duplicate from `delivered` was acknowledged without replaying the recorded result")
        self.assertEqual(json.loads(reply.read_text()).get("value"), "done",
                         f"the caller's outbox holds a foreign reply instead of the recorded result: {reply.read_text()}")

    # -- probes that hold (documented behaviour made visible) ------------------
    def test_expired_lease_on_a_live_owner_is_a_crash_window_and_the_late_owner_converges(self):
        """Guarantee 15 / runbook: a claim older than claim_lease_s whose owner
        is still alive is treated as a crash window. The second consumer
        answers uncertain and acknowledges; when the slow owner finishes it
        overwrites the record with the real result, republishes, meets an
        already-acknowledged request (ack_already_gone) and the record ends
        `acknowledged` with the real result. One execution, two replies, one
        OUTCOME_UNCERTAIN alert: documented, not exactly-once."""
        self.queue()
        first = self.agent
        second = self.make_agent(UnlockedWorker)
        rec_path = self.record_path()

        def first_handle(req):
            first.calls.append(req)
            rec = json.loads(rec_path.read_text())
            stale = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            rec["owner"]["claimed_at"] = stale                  # the handler has been running for two hours
            tmp = rec_path.with_name(rec_path.name + ".tmp")
            tmp.write_text(json.dumps(rec))
            tmp.replace(rec_path)
            second.tick()
            return {"ok": True, "value": "done"}

        first.handle = first_handle
        first.tick()
        self.assertEqual(len(first.calls) + len(second.calls), 1)
        self.assertEqual(len(self.events("outcome_uncertain")), 1)
        self.assertEqual(len(self.events("ack_already_gone")), 1)
        self.assertEqual(list(self.inbox().glob("*.json")), [])
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(self.outcome()["result"].get("value"), "done")
        self.assertEqual(self.response().get("value"), "done")

    def test_crash_between_claim_and_dispatch_is_answered_uncertain_not_rerun(self):
        """Guarantee 15 (at-most-once, as documented): the claim is durable, the
        process dies before the handler starts. The next boot cannot tell this
        from a crash after a side effect and answers uncertain without running."""
        self.queue()
        with patch.object(self.agent, "_dispatch", side_effect=SimulatedExit("killed right after the claim")):
            with self.assertRaises(SimulatedExit):
                self.agent.tick()
        self.assertEqual(self.outcome()["status"], "executing")
        self.assertFalse((self.agent.self_dir / "state" / "current_op.json").exists())
        self.release(self.agent)
        self.agent = self.make_agent()
        self.agent.tick()
        self.assertEqual(self.agent.calls, [])
        self.assertEqual(self.response()["outcome"], "uncertain")
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(len(self.events("outcome_uncertain")), 1)

    def test_reply_tmp_left_by_a_crash_inside_respond_does_not_block_redelivery(self):
        """Guarantee 2/12: respond() dies after creating <eid>.json.tmp in the
        caller's outbox and before the rename. The record is `recorded`; the
        next tick must unlink the leftover, redeliver and acknowledge without
        running the handler."""
        self.queue()
        reply = comms.outbox_dir("client") / "001.json"
        real_replace = os.replace

        def die_on_reply_rename(src, dst, *a, **k):
            if str(dst) == str(reply):
                raise SimulatedExit("power loss between the reply tmp and its rename")
            return real_replace(src, dst, *a, **k)

        with patch.object(os, "replace", side_effect=die_on_reply_rename):
            with self.assertRaises(SimulatedExit):
                self.agent.tick()
        self.assertTrue(reply.with_name(reply.name + ".tmp").exists())
        self.assertFalse(reply.exists())
        self.assertEqual(self.outcome()["status"], "recorded")
        self.release(self.agent)
        self.agent = self.make_agent()
        self.agent.tick()
        self.assertEqual(self.agent.calls, [])
        self.assertEqual(self.response()["value"], "done")
        self.assertFalse(reply.with_name(reply.name + ".tmp").exists())
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(list(self.inbox().glob("*.json")), [])

    def test_recorded_write_failure_then_storage_recovery_does_not_double_execute_within_the_lease(self):
        """Guarantee 15: the `recorded` write fails (storage), the reply and the
        acknowledgement still go out, the durable record says `executing` under
        this pid with a fresh claim. A same-input resend inside the lease meets
        our own pid with no current_op.json: not a live claim, a crash window.
        Uncertain, never a second run, and the record ends acknowledged."""
        self.queue()
        real_save = self.agent._save_outcome

        def fail_after_claim(eid, rec):
            if rec.get("status") != "executing":
                return False
            return real_save(eid, rec)

        with patch.object(self.agent, "_save_outcome", side_effect=fail_after_claim):
            self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(self.outcome()["status"], "executing")
        self.assertEqual(self.outcome()["owner"]["pid"], os.getpid())
        for _ in range(2):
            self.queue()
            self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1, "handler re-ran after a storage failure")
        self.assertEqual(self.response()["outcome"], "uncertain")
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(list(self.inbox().glob("*.json")), [])

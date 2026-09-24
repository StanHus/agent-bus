"""Adversarial tests, lens: breaker-clock.

Breaker state machine and scheduling: threshold edges, half-open probe
semantics, clock changes, monotonic use, which failures count, heal while open,
_maybe_dream at hour boundaries, per-tick budget.

    /opt/homebrew/bin/python3 -m unittest tests.adversarial_breaker_clock -v

Reuses the sandboxed BusTestCase from tests/test_contracts.py: every bus root is
redirected into a temporary directory, os._exit is a mock, nothing under ~ is
touched. `BreakerClockBreaks` holds the cases that fail against the current
code (each docstring names the guarantee in CONTRACT.md it attacks);
`BreakerClockHolds` records the attacks that did not get through, so a fixer
can see the boundary.

Round 2: every round-1 break now holds (those classes stay as pins).
`BreakerClockBreaksRound2` holds the new confirmed breaks: a delivery failure
overwrites the handler's breaker class (a failed probe does not reopen, a
counted failure is never counted, a successful probe never closes), a dream
attempt is not persisted before the run (a dream that kills the process retries
without bound), and a slot that ran and failed is rewritten as `missed` when
its day ends. `BreakerClockHoldsRound2` pins the round-2 attacks that held.

Round 3: every round-2 break now holds. `BreakerClockBreaksRound3` holds the
new confirmed breaks: a heal behind a paused backlog that exhausts the time
budget is never admitted while open (recovery unreachable), the slot of a day
skipped by downtime is never recorded as missed, the reopen alert from a
storage-failed probe carries no threshold value, and a rejection probe lets a
second message run while half open. `BreakerClockHoldsRound3` pins the
round-3 attacks that held (cooldown expiring mid-batch, storage on the probe's
delivered record, duplicate replay while half open, doubling base untouched by
failures counted while open, budget/health clock sources, dream at the top of
the hour and while open).
"""
import itertools
import json
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone, time as dtime
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import agent_base  # noqa: E402
from tests.test_contracts import BusTestCase, SimulatedExit, Worker  # noqa: E402  (no test methods on these)


class BreakerClockCase(BusTestCase):
    def trip(self):
        """Five counted failures: closed -> open with the base 300 s cooldown."""
        for n in range(5):
            self.queue(eid=f"f{n:02}")
        self.agent.result = {"ok": False, "error": "down"}
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertEqual(self.agent._consec_failures, 5)
        self.assertEqual(self.agent._breaker_cooldown_s, 300)
        self.agent.result = {"ok": True}

    def expire_cooldown(self):
        self.agent._circuit_breaker_pause_until = time.monotonic() - 1

    def slots(self):
        return json.loads((self.agent.self_dir / "state" / "dream_slots.json").read_text())


class BreakerClockBreaks(BreakerClockCase):
    """Cases that FAIL against the current code."""

    def test_counted_failure_while_open_does_not_restart_the_cooldown(self):
        """CONTRACT 18: closed -> open on trip, open -> half_open when the cooldown
        passes. There is no open -> open edge. A heal is the only kind admitted
        while open (19); if it fails, the failure is counted and, because
        _consec_failures is already at the threshold, _trip_breaker runs again
        and moves _circuit_breaker_pause_until to now + cooldown. 290 elapsed
        seconds of the pause are thrown away and a second trip is recorded."""
        self.trip()
        self.assertEqual(len(self.events("circuit_breaker_tripped")), 1)
        # 290 s of the 300 s cooldown have elapsed.
        self.agent._circuit_breaker_pause_until = time.monotonic() + 10
        self.queue(eid="h00", kind="heal", action="rewrite_north_star")
        self.agent.healing_handler = Mock(side_effect=RuntimeError("heal broke"))
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "open")
        remaining = self.agent._circuit_breaker_pause_until - time.monotonic()
        self.assertLessEqual(remaining, 10.5,
                             f"a failed heal restarted the cooldown: {remaining:.0f}s left, expected <= 10s")
        self.assertEqual(len(self.events("circuit_breaker_tripped")), 1,
                         "no second trip may be recorded for an open -> open transition")

    def test_half_open_probe_whose_record_cannot_be_written_reopens(self):
        """CONTRACT 18: half open admits exactly one probe; a counted failure
        reopens with the cooldown doubled. CONTRACT 10: storage (record could
        not be written) counts. The executing-record write failure path only
        tests _consec_failures >= threshold and ignores _breaker_state, so when
        the counter was reset by a heal that succeeded while open, the probe
        fails on storage, nothing reopens, and the next tick admits another
        probe. Four more probes are admitted before the breaker notices."""
        self.trip()
        # An operator heals while open: success resets the counter, breaker stays open.
        self.queue(eid="h00", kind="heal", action="rewrite_north_star")
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertEqual(self.agent._consec_failures, 0)
        self.expire_cooldown()
        self.queue(eid="p00")
        with patch.object(self.agent, "_save_outcome", return_value=False):
            self.agent.tick()
        self.assertEqual(len(self.agent.calls), 5, "the probe must not run without a record")
        self.assertTrue((self.inbox() / "p00.json").exists())
        self.assertEqual(self.agent._breaker_state, "open",
                         "a counted storage failure on the half-open probe must reopen the breaker")
        self.assertEqual(self.agent._breaker_cooldown_s, 600)

    def test_half_open_does_not_run_idle_work_on_every_tick_without_a_probe(self):
        """CONTRACT 17: idle_cycle is skipped while open. CONTRACT 18 / runbook:
        when the cooldown passes one probe is admitted and success closes it.
        With an empty inbox nothing can ever close the breaker, yet
        _circuit_breaker_open() returns False in half_open, so the paused idle
        work resumes on every tick indefinitely while health.json still reports
        the breaker as half_open. Idle failures never count, so the breaker can
        never observe the outcome of that work either."""
        self.trip()
        self.assertEqual(self.agent.idle_calls, 0)
        self.expire_cooldown()
        for _ in range(3):
            self.agent.tick()          # empty inbox: no probe can run
        self.assertEqual(self.agent._breaker_state, "half_open")
        self.assertEqual(self.health()["breaker"]["state"], "half_open")
        self.assertLessEqual(self.agent.idle_calls, 1,
                             f"idle_cycle ran {self.agent.idle_calls} times in half_open with no probe admitted")

    def test_dream_window_that_crosses_midnight_still_runs_after_midnight(self):
        """CONTRACT 27: windows are [start, start + 5 min] (timedelta, so 23:58
        works). The slot key and window are computed from now_utc.date(), so at
        00:01 the 23:58 slot of the previous day is never examined: it is inside
        its window but is neither run, nor caught up, nor recorded as missed.
        The existing test only checks 23:59:30, before the date rolls."""
        self.agent.dream_times_utc = (dtime(23, 58),)
        self.agent.dream_sequence = Mock()
        inside_window = datetime(2026, 9, 25, 0, 1, 0, tzinfo=timezone.utc)   # 23:58 + 3 min
        self.agent._maybe_dream(now_utc=inside_window)
        self.agent.dream_sequence.assert_called_once()
        self.assertEqual(self.slots()["2026-09-24T23:58"]["status"], "completed")

    def test_slot_never_run_before_the_day_ends_is_recorded_as_missed(self):
        """CONTRACT 27: a slot missed while busy is run once later the same UTC
        day when dream_catch_up is true, otherwise recorded as missed.
        state/dream_slots.json is the operator's record of the last 7 days. Once
        the date rolls, yesterday's never-run slot is outside the loop, so the
        file carries no entry at all: not completed, not failed, not missed."""
        self.agent.dream_times_utc = (dtime(6, 30),)
        self.agent.dream_sequence = Mock()
        next_day = datetime(2026, 9, 25, 0, 30, tzinfo=timezone.utc)
        self.agent._maybe_dream(now_utc=next_day)
        self.agent.dream_sequence.assert_not_called()   # catch-up is same-day only: correct
        slots_path = self.agent.self_dir / "state" / "dream_slots.json"
        self.assertTrue(slots_path.exists(), "the skipped slot left no record at all")
        self.assertEqual(self.slots().get("2026-09-24T06:30", {}).get("status"), "missed")

    def test_record_write_failure_after_the_reply_counts_as_storage(self):
        """CONTRACT 10: storage (record, reply or acknowledgement could not be
        written) counts. The `delivered` and `acknowledged` record writes ignore
        _save_outcome's return value, so a volume that stops accepting writes
        after the `recorded` write is invisible to the breaker: the message is
        classed success, the counter resets, and the on-disk record disagrees
        with the acknowledged (deleted) request."""
        self.agent._consec_failures = 4
        real = self.agent._save_outcome

        def after_reply_fails(eid, rec):
            if rec.get("status") in ("delivered", "acknowledged"):
                return False
            return real(eid, rec)

        self.queue(eid="s00")
        with patch.object(self.agent, "_save_outcome", side_effect=after_reply_fails):
            self.agent.tick()
        self.assertFalse((self.inbox() / "s00.json").exists())
        self.assertEqual(self.outcome("s00")["status"], "recorded", "on disk the record never advanced")
        self.assertTrue(self.agent._circuit_breaker_open(),
                        "the fifth consecutive counted failure (storage) must trip the breaker")

    def test_health_breaker_reason_is_cleared_when_the_breaker_closes(self):
        """Monitoring table: health.breaker carries state, until_s, reason;
        open/half_open -> paused. After the probe succeeds and the breaker
        closes, _breaker_reason is never cleared, so health.json reports
        state closed together with the trip reason (cosmetic)."""
        self.trip()
        self.expire_cooldown()
        self.queue(eid="p00")
        self.agent.tick()
        breaker = self.health()["breaker"]
        self.assertEqual(breaker["state"], "closed")
        self.assertEqual(breaker["until_s"], 0)
        self.assertIsNone(breaker["reason"], f"stale reason on a closed breaker: {breaker['reason']!r}")


class BreakerClockHolds(BreakerClockCase):
    """Attacks that did not get through; kept as pins for the fixer."""

    def test_breaker_deadline_ignores_wall_clock_jumps(self):
        self.trip()
        with patch.object(agent_base.time, "time", return_value=time.time() + 86400):
            self.queue(eid="w00")
            self.agent.tick()
        self.assertTrue((self.inbox() / "w00.json").exists())
        self.assertEqual(self.agent._breaker_state, "open")

    def test_cooldown_doubles_and_caps_at_3600(self):
        self.trip()
        self.agent.result = {"ok": False, "error": "down"}
        seen = []
        for n in range(5):
            self.expire_cooldown()
            self.queue(eid=f"r{n:02}")
            self.agent.tick()
            seen.append(self.agent._breaker_cooldown_s)
        self.assertEqual(seen, [600, 1200, 2400, 3600, 3600])

    def test_rejection_during_half_open_is_not_a_probe_outcome(self):
        self.trip()
        self.expire_cooldown()
        self.queue(eid="r00")
        self.agent.result = {"ok": False, "error": "bad request", "failure_class": "rejection"}
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "half_open")
        self.assertEqual(self.agent._breaker_cooldown_s, 300)

    def test_threshold_edge_four_failures_then_success_resets(self):
        for n in range(4):
            self.queue(eid=f"f{n:02}")
        self.agent.result = {"ok": False, "error": "down"}
        self.agent.tick()
        self.assertEqual(self.agent._consec_failures, 4)
        self.assertEqual(self.agent._breaker_state, "closed")
        self.agent.result = {"ok": True}
        self.queue(eid="ok0")
        self.agent.tick()
        self.assertEqual(self.agent._consec_failures, 0)

    def test_half_open_first_probe_failure_shields_the_rest_of_the_batch(self):
        self.trip()
        self.expire_cooldown()
        self.agent.result = {"ok": False, "error": "down"}
        for n in range(3):
            self.queue(eid=f"p{n:02}")
        self.agent.tick()
        self.assertEqual([r["event_id"] for r in self.agent.calls][5:], ["p00"])
        self.assertEqual(self.agent._breaker_state, "open")

    def test_message_budget_counts_only_admitted_work_while_open(self):
        self.trip()
        self.agent.max_messages_per_tick = 1
        for n in range(3):
            self.queue(eid=f"w{n:02}")
        self.queue(eid="zheal", kind="heal", action="rewrite_north_star")
        self.agent.tick()
        self.assertFalse((self.inbox() / "zheal.json").exists(), "skipped work must not eat the budget")
        self.assertEqual(len(list(self.inbox().glob("w*.json"))), 3)


class BreakerClockBreaksRound2(BreakerClockCase):
    """Round 2: cases that FAIL against the current code."""

    def outbox_broken(self):
        return patch.object(agent_base, "respond", side_effect=OSError("outbox unwritable"))

    def test_half_open_probe_failure_reopens_even_when_its_reply_cannot_be_delivered(self):
        """CONTRACT 18: half open admits exactly one probe; a counted failure
        (a failing handler) reopens with the cooldown doubled. CONTRACT 10:
        `delivery` does not count, `failure` does. In _process_message the
        handler's class is computed first and then overwritten by
        `klass = "delivery"` when respond raises, so the record says
        outcome_class failure while the breaker is told delivery: the probe's
        failure is dropped, the breaker stays half_open and the very next file in
        the same batch is admitted as a second probe against the dead dependency."""
        self.trip()
        self.expire_cooldown()
        self.agent.result = {"ok": False, "error": "down"}
        self.queue(eid="p00")
        self.queue(eid="p01")
        with self.outbox_broken():
            self.agent.tick()
        self.assertEqual(self.outcome("p00")["outcome_class"], "failure")
        self.assertEqual([r["event_id"] for r in self.agent.calls][5:], ["p00"],
                         "exactly one probe may run while half open")
        self.assertEqual(self.agent._breaker_state, "open",
                         "the probe failed: the breaker must reopen regardless of the reply's fate")
        self.assertEqual(self.agent._breaker_cooldown_s, 600)

    def test_handler_failure_counts_toward_the_threshold_when_the_reply_cannot_be_delivered(self):
        """CONTRACT 10: `failure` (plain ok: false) counts; `delivery` is audited,
        retried next tick and does not count. When both happen on one message
        the delivery class wins and the handler failure is never counted: not
        this tick (klass overwritten) and not on redelivery (klass None, "already
        counted on an earlier tick"). A caller with a broken outbox therefore
        keeps the breaker from ever tripping while the dependency fails 50 times
        per tick."""
        self.agent._consec_failures = 4
        self.agent.result = {"ok": False, "error": "down"}
        self.queue(eid="f04")
        with self.outbox_broken():
            self.agent.tick()
        self.assertEqual(self.outcome("f04")["outcome_class"], "failure")
        self.assertEqual(self.outcome("f04")["attempts"]["deliver"], 1)
        self.assertEqual(self.agent._consec_failures, 5,
                         "the fifth consecutive handler failure was not counted because its reply failed")
        self.assertEqual(self.agent._breaker_state, "open")
        # Redelivery of the same recorded outcome must not count it a second time.
        self.expire_cooldown()
        self.agent.tick()
        self.assertEqual(self.outcome("f04")["status"], "acknowledged")
        self.assertEqual(self.agent._consec_failures, 5)

    def test_successful_probe_closes_the_breaker_once_its_reply_is_delivered(self):
        """CONTRACT 18: half open admits exactly one probe: success closes. The
        probe's handler succeeds (record: outcome_class success) but the caller's
        outbox is unwritable this tick, so the class becomes `delivery` and the
        breaker learns nothing. Next tick the reply is redelivered from the record
        and acknowledged with klass None, so the success is never observed
        either: the breaker stays half_open, idle work stays paused, and only an
        unrelated later request can close it."""
        self.trip()
        self.expire_cooldown()
        self.queue(eid="p00")
        with self.outbox_broken():
            self.agent.tick()
        self.assertEqual(self.outcome("p00")["outcome_class"], "success")
        self.agent.tick()                      # reply redelivered from the record, request acknowledged
        self.assertEqual(self.outcome("p00")["status"], "acknowledged")
        self.assertIs(self.response("p00")["ok"], True)
        self.assertEqual(len(self.agent.calls), 6, "the handler ran once")
        self.assertEqual(self.agent._breaker_state, "closed",
                         "a probe that succeeded and whose reply reached the caller must close the breaker")

    def test_dream_attempt_is_persisted_before_the_run_so_a_crashing_dream_stops_after_three(self):
        """CONTRACT 27: a failed slot retries at most three times; completed
        slots persist so a restart inside the window does not repeat one. The
        attempt counter is incremented in memory and only written after
        dream_sequence returns, so a dream that takes the process down (OOM
        kill, os._exit inside a skill, SIGKILL by the supervisor) leaves attempts
        at 0 on disk. Every boot inside the window (or the catch-up day) runs it
        again: a crash loop with no bound, and no failed/missed record."""
        self.agent.dream_times_utc = (dtime(6, 30),)
        self.agent.dream_sequence = Mock(side_effect=SimulatedExit)
        now = datetime(2026, 9, 24, 6, 31, tzinfo=timezone.utc)
        for _ in range(5):                     # five boots inside the window
            try:
                self.agent._maybe_dream(now_utc=now)
            except SimulatedExit:
                pass
        self.assertLessEqual(self.agent.dream_sequence.call_count, 3,
                             f"a dream that kills the process was run {self.agent.dream_sequence.call_count} times")
        slot = self.slots().get("2026-09-24T06:30", {})
        self.assertGreaterEqual(slot.get("attempts", 0), 1, "no attempt was recorded on disk")

    def test_failed_dream_slot_is_not_rewritten_as_missed_when_its_day_ends(self):
        """CONTRACT 27: state/dream_slots.json shows every slot of the last 7
        days as completed/failed/missed. A slot that ran twice and failed (audit
        dream_sequence_failed x2) is `failed`, attempts 2. Once the UTC day rolls
        the yesterday branch sees now > window_end with catch-up not applicable
        and unconditionally rewrites the slot as `missed`, so the operator's
        record contradicts the audit trail (the same happens inside the day with
        dream_catch_up off, five minutes after the window)."""
        self.agent.dream_times_utc = (dtime(23, 50),)
        self.agent.dream_sequence = Mock(side_effect=RuntimeError("dream broke"))
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 24, 23, 51, tzinfo=timezone.utc))
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 24, 23, 53, tzinfo=timezone.utc))
        before = self.slots()["2026-09-24T23:50"]
        self.assertEqual((before["status"], before["attempts"]), ("failed", 2))
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 25, 0, 10, tzinfo=timezone.utc))
        self.assertEqual(self.agent.dream_sequence.call_count, 2, "yesterday's slot must not be caught up")
        after = self.slots()["2026-09-24T23:50"]
        self.assertEqual(after["status"], "failed",
                         f"a slot that ran and failed was rewritten as {after['status']!r}")
        self.assertEqual(after["attempts"], 2)


class BreakerClockHoldsRound2(BreakerClockCase):
    """Round 2 attacks that did not get through; kept as pins."""

    def test_rejection_between_failures_neither_counts_nor_resets(self):
        for n in range(4):
            self.queue(eid=f"f{n:02}")
        self.agent.result = {"ok": False, "error": "down"}
        self.agent.tick()
        self.assertEqual(self.agent._consec_failures, 4)
        self.queue(eid="rej")
        self.agent.result = {"ok": False, "error": "bad", "failure_class": "rejection"}
        self.agent.tick()
        self.assertEqual(self.agent._consec_failures, 4)
        self.assertEqual(self.agent._breaker_state, "closed")
        self.queue(eid="f04")
        self.agent.result = {"ok": False, "error": "down"}
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "open")

    def test_open_to_half_open_is_audited_once_per_open_period(self):
        self.trip()
        self.expire_cooldown()
        for n in range(3):
            self.queue(eid=f"w{n:02}")
        self.agent.result = {"ok": False, "error": "bad", "failure_class": "rejection"}
        self.agent.tick()
        self.agent.tick()
        self.assertEqual(len(self.events("circuit_breaker_half_open")), 1)

    def test_heal_success_while_open_resets_the_counter_but_keeps_the_pause(self):
        self.trip()
        self.agent._circuit_breaker_pause_until = time.monotonic() + 100
        self.queue(eid="h00", kind="heal", action="rewrite_north_star")
        self.queue(eid="w00")
        self.agent.tick()
        self.assertEqual(self.agent._consec_failures, 0)
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertTrue((self.inbox() / "w00.json").exists())
        self.assertEqual(self.agent.idle_calls, 0)
        self.assertGreater(self.health()["breaker"]["until_s"], 90)

    def test_dream_window_end_is_inclusive_and_next_second_is_late(self):
        self.agent.dream_times_utc = (dtime(6, 30),)
        self.agent.dream_catch_up = False
        self.agent.dream_sequence = Mock()
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 24, 6, 35, 0, tzinfo=timezone.utc))
        self.agent.dream_sequence.assert_called_once()
        self.assertFalse(self.slots()["2026-09-24T06:30"]["late"])
        self.agent.dream_times_utc = (dtime(7, 30),)
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 24, 7, 35, 1, tzinfo=timezone.utc))
        self.assertEqual(self.slots()["2026-09-24T07:30"]["status"], "missed")

    def test_two_slots_inside_one_window_run_on_consecutive_ticks_not_one(self):
        self.agent.dream_times_utc = (dtime(23, 58), dtime(0, 0))
        self.agent.dream_sequence = Mock()
        now = datetime(2026, 9, 25, 0, 2, tzinfo=timezone.utc)
        self.agent._maybe_dream(now_utc=now)
        self.assertEqual(self.agent.dream_sequence.call_count, 1)
        self.agent._maybe_dream(now_utc=now + timedelta(minutes=1))
        self.assertEqual(self.agent.dream_sequence.call_count, 2)
        statuses = {k: v["status"] for k, v in self.slots().items()}
        self.assertEqual(statuses["2026-09-24T23:58"], "completed")
        self.assertEqual(statuses["2026-09-25T00:00"], "completed")
        self.assertEqual(statuses["2026-09-24T00:00"], "missed")   # yesterday's slot, no history: correct

    def test_time_budget_break_leaves_the_remainder_and_still_runs_maintenance(self):
        for n in range(3):
            self.queue(eid=f"b{n:02}")
        base = time.monotonic()
        ticks = iter([base, base, base + 100, base + 100, base + 100, base + 100, base + 100])
        with patch.object(agent_base.time, "monotonic", side_effect=lambda: next(ticks, base + 100)):
            self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(len(list(self.inbox().glob("*.json"))), 2)
        self.assertEqual(self.events("tick_budget_exhausted")[0]["remaining"], 2)
        self.assertEqual(self.agent.idle_calls, 1)
        self.heartbeat.assert_called_once()

    def test_breaker_open_skips_do_not_consume_the_time_budget_check_order(self):
        self.trip()
        for n in range(3):
            self.queue(eid=f"w{n:02}")
        self.queue(eid="zheal", kind="heal", action="rewrite_north_star")
        self.agent.tick()
        self.assertFalse((self.inbox() / "zheal.json").exists())
        self.assertFalse(self.events("tick_budget_exhausted"))


class BreakerClockBreaksRound3(BreakerClockCase):
    """Round 3: cases that FAIL against the current code."""

    def test_heal_stays_reachable_while_open_behind_a_paused_backlog(self):
        """CONTRACT 19: while open, `kind: heal` requests are still processed;
        everything else waits in the inbox. CONTRACT 26: only tick_budget_s
        seconds of draining per tick. The drain walks the inbox in name order
        and every paused (non-heal) file it passes over is charged to the time
        budget although nothing is done with it, so once the backlog ahead of
        the heal takes longer to walk than tick_budget_s the same prefix is
        re-read on every tick and the heal at the end is never reached: the
        runbook's own remedy (send `heal: rewrite_north_star`) cannot arrive,
        and neither can `heal: respawn`. The clock is patched to advance one
        second per reading so the walk is deterministic; the shape is the
        same at real speed with a larger backlog."""
        self.trip()
        self.agent._circuit_breaker_pause_until = 10 ** 12   # open on any clock value below
        for n in range(6):
            self.queue(eid=f"w{n:02}")
        self.queue(eid="zz-heal", kind="heal", action="rewrite_north_star")
        clock = itertools.count(0.0, 1.0)
        with patch.object(agent_base.time, "monotonic", side_effect=lambda: next(clock)):
            for _ in range(10):
                self.agent.tick()
        self.assertEqual(len(list(self.inbox().glob("w*.json"))), 6, "paused work must stay put")
        self.assertFalse((self.inbox() / "zz-heal.json").exists(),
                         "a heal behind a paused backlog was never admitted in ten ticks")
        self.assertEqual(self.response("zz-heal")["healed"], "north_star_regen")

    def test_slots_of_a_day_skipped_by_downtime_are_recorded_as_missed(self):
        """CONTRACT 27: a slot never run when its UTC day ends is recorded as
        `missed`, so the file shows every slot of the last 7 days as
        completed/failed/missed. Only yesterday's and today's slots are
        examined, so after 48 h of downtime (host off, supervisor wedged) the
        slot of the day in between is neither completed, failed nor missed:
        the operator's 7-day record has a silent hole exactly where the outage
        was."""
        self.agent.dream_times_utc = (dtime(6, 30),)
        self.agent.dream_sequence = Mock()
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 21, 6, 31, tzinfo=timezone.utc))
        self.assertEqual(self.slots()["2026-09-21T06:30"]["status"], "completed")
        # Down through the 22nd and 23rd; first tick back on the 24th.
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 24, 5, 0, tzinfo=timezone.utc))
        statuses = {k: v["status"] for k, v in self.slots().items()}
        self.assertEqual(statuses.get("2026-09-23T06:30"), "missed")
        self.assertEqual(statuses.get("2026-09-22T06:30"), "missed",
                         f"the slot of the day skipped by downtime left no record: {sorted(statuses)}")

    def test_reopen_alert_from_a_storage_probe_carries_the_threshold_value(self):
        """CONTRACT 18: state/breaker.json records each trip; the alert body has
        real newlines and the threshold value. The half-open probe whose
        `executing` record cannot be written reopens through
        _count_failure("outcome record could not be written"); that reason
        names neither the threshold nor a count, and after a heal that
        succeeded while open the consecutive count is 1, so the alert an
        operator reads for this trip carries no threshold value at all."""
        self.trip()
        self.queue(eid="h00", kind="heal", action="rewrite_north_star")
        self.agent.tick()                       # success while open: counter reset, still open
        self.assertEqual(self.agent._consec_failures, 0)
        self.expire_cooldown()
        self.queue(eid="p00")
        with patch.object(self.agent, "_save_outcome", return_value=False):
            self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertEqual(self.agent._breaker_cooldown_s, 600)
        bodies = [p.read_text() for p in (self.tmp / "to_main").glob("*CIRCUIT_BREAKER_TRIPPED*")]
        (body,) = [b for b in bodies if "**cooldown_s**: 600" in b]
        self.assertIn("\n", body)
        self.assertIn(str(self.agent._circuit_breaker_threshold), body,
                      f"the reopen alert carries no threshold value:\n{body}")

    def test_half_open_admits_one_handler_run_even_when_the_first_is_a_rejection(self):
        """CONTRACT 17/18: half open admits exactly one probe. A probe that the
        handler classes `rejection` is neither a success nor a counted failure,
        so the breaker stays half_open and, because _circuit_breaker_open()
        returns False in that state, the very next file of the same batch runs
        too: two handler runs (and, had the second been another rejection,
        fifty) while the breaker is half open. The bound is on probes admitted,
        not on verdicts received."""
        self.trip()
        self.expire_cooldown()
        results = {"r00": {"ok": False, "error": "bad request", "failure_class": "rejection"}}

        def handle(req):
            self.agent.calls.append(req)
            return results.get(req["event_id"], {"ok": False, "error": "down"})

        self.agent.handle = handle
        for eid in ("r00", "w01", "w02"):
            self.queue(eid=eid)
        self.agent.tick()
        self.assertEqual([r["event_id"] for r in self.agent.calls][5:], ["r00"],
                         "exactly one message may run while the breaker is half open")
        self.assertEqual(self.agent._breaker_state, "half_open")


class BreakerClockHoldsRound3(BreakerClockCase):
    """Round 3 attacks that did not get through; kept as pins."""

    def test_cooldown_expiring_mid_batch_admits_the_next_file_as_the_probe(self):
        self.trip()
        base = time.monotonic()
        self.agent._circuit_breaker_pause_until = base + 2.5
        for eid in ("w00", "w01", "w02"):
            self.queue(eid=eid)
        clock = itertools.count(base, 1.0)     # started, check, breaker, check, breaker, ...
        with patch.object(agent_base.time, "monotonic", side_effect=lambda: next(clock)):
            self.agent.tick()
        # w00 skipped (t=base+2 < deadline), w01 admitted as the probe (t=base+4), succeeds, closes.
        self.assertEqual([r["event_id"] for r in self.agent.calls][5:], ["w01", "w02"])
        self.assertTrue((self.inbox() / "w00.json").exists())
        self.assertEqual(self.agent._breaker_state, "closed")
        self.assertEqual(len(self.events("circuit_breaker_half_open")), 1)

    def test_storage_failure_on_the_probes_delivered_record_reopens(self):
        self.trip()
        self.expire_cooldown()
        real = self.agent._save_outcome

        def delivered_write_fails(eid, rec):
            if rec.get("status") == "delivered":
                return False
            return real(eid, rec)

        self.queue(eid="p00")
        with patch.object(self.agent, "_save_outcome", side_effect=delivered_write_fails):
            self.agent.tick()
        self.assertIs(self.response("p00")["ok"], True)
        self.assertEqual(self.agent._breaker_state, "open", "storage overrides the probe's success")
        self.assertEqual(self.agent._breaker_cooldown_s, 600)

    def test_duplicate_replay_of_an_old_success_is_not_a_probe_outcome(self):
        self.queue(eid="old")
        self.agent.tick()
        self.assertEqual(self.outcome("old")["status"], "acknowledged")
        self.trip()
        self.expire_cooldown()
        self.queue(eid="old")                   # same input resent: replayed, handler not run
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 6)
        self.assertEqual(self.agent._breaker_state, "half_open")

    def test_failures_counted_while_open_do_not_change_the_doubling_base(self):
        self.trip()
        self.agent._circuit_breaker_pause_until = time.monotonic() + 100
        self.agent.healing_handler = Mock(side_effect=RuntimeError("heal broke"))
        for n in range(3):
            self.queue(eid=f"h{n:02}", kind="heal", action="rewrite_north_star")
            self.agent.tick()
        self.assertEqual(self.agent._consec_failures, 8)
        self.assertEqual(self.agent._breaker_cooldown_s, 300)
        self.assertEqual(len(self.events("circuit_breaker_tripped")), 1)
        self.expire_cooldown()
        self.agent.result = {"ok": False, "error": "down"}
        self.queue(eid="p00")
        self.agent.tick()
        self.assertEqual(self.agent._breaker_cooldown_s, 600, "the first reopen doubles from the base once")

    def test_consecutive_failures_accumulate_across_ticks(self):
        self.agent.result = {"ok": False, "error": "down"}
        for n in range(4):
            self.queue(eid=f"f{n:02}")
            self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "closed")
        self.queue(eid="f04")
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertFalse((self.inbox() / "f04.json").exists(), "the fifth is still acknowledged")

    def test_explicit_tick_budget_is_honoured_over_the_cadence_default(self):
        self.agent.tick_budget_s = 0.5
        for n in range(3):
            self.queue(eid=f"b{n:02}")
        clock = itertools.count(0.0, 0.3)
        with patch.object(agent_base.time, "monotonic", side_effect=lambda: next(clock)):
            self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(self.events("tick_budget_exhausted")[0]["remaining"], 2)

    def test_health_until_s_is_monotonic_not_wall_clock(self):
        self.trip()
        with patch.object(agent_base.time, "time", return_value=time.time() + 10 ** 6):
            self.agent.tick()
        self.assertGreater(self.health()["breaker"]["until_s"], 290)
        self.assertEqual(self.health()["breaker"]["state"], "open")

    def test_dream_window_crossing_the_top_of_the_hour_runs_and_keys_by_start(self):
        self.agent.dream_times_utc = (dtime(6, 58),)
        self.agent.dream_sequence = Mock()
        self.agent._maybe_dream(now_utc=datetime(2026, 9, 24, 7, 2, 59, tzinfo=timezone.utc))
        self.agent.dream_sequence.assert_called_once()
        self.assertEqual(self.slots()["2026-09-24T06:58"]["status"], "completed")
        self.assertFalse(self.slots()["2026-09-24T06:58"]["late"])

    def test_dream_runs_while_the_breaker_is_open_and_idle_does_not(self):
        self.trip()
        self.agent.dream_times_utc = (dtime(6, 30),)
        self.agent.dream_sequence = Mock()
        real = self.agent._maybe_dream
        # A date the wall clock cannot reach: trip() already ticked with the real clock.
        inside = datetime(2030, 1, 15, 6, 31, tzinfo=timezone.utc)
        with patch.object(self.agent, "_maybe_dream", side_effect=lambda now_utc=None: real(now_utc=inside)):
            self.agent.tick()
        self.agent.dream_sequence.assert_called_once()
        self.assertEqual(self.slots()["2030-01-15T06:30"]["status"], "completed")
        self.assertEqual(self.agent.idle_calls, 0)

    def test_breaker_json_records_the_reopen_with_the_doubled_cooldown(self):
        self.trip()
        self.expire_cooldown()
        self.agent.result = {"ok": False, "error": "down"}
        self.queue(eid="p00")
        self.agent.tick()
        rec = json.loads((self.agent.self_dir / "state" / "breaker.json").read_text())
        self.assertEqual((rec["state"], rec["cooldown_s"]), ("open", 600))
        self.assertEqual(len(self.events("circuit_breaker_tripped")), 2)


if __name__ == "__main__":
    unittest.main()

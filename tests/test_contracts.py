"""Contract tests for the agent-bus lifecycle, run against the real modules.

    /opt/homebrew/bin/python3 -m unittest tests.test_contracts -v

Every filesystem root the bus knows (AGENTS_ROOT, AUDIT_DIR, heartbeat, bus
health, to_main) is redirected into a temporary directory per test, so nothing
here touches the real agent tree or iCloud. Each test names the guarantee it
pins; CONTRACT.md lists the same guarantees in prose.
"""
import json
import os
import shutil
import sys
import tempfile
import time
import types
import unittest
from datetime import datetime, timezone, time as dtime
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import comms          # noqa: E402
import agent_base     # noqa: E402
from agent_base import BaseAgent  # noqa: E402


class SimulatedExit(BaseException):
    """Stands in for os._exit so the test process survives."""


class Worker(BaseAgent):
    name = "worker"
    cadence_s = 0

    def __init__(self):
        self.calls = []
        self.result = {"ok": True, "value": "done"}
        self.raise_exc = None
        self.idle_calls = 0
        super().__init__()

    def handle(self, req):
        self.calls.append(req)
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.result

    def idle_cycle(self):
        self.idle_calls += 1


class BusTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="agent-bus-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        roots = {
            "AGENTS_ROOT": self.tmp / "agents", "AUDIT_DIR": self.tmp / "audit",
            "HB_DIR": self.tmp / "hb", "LOCAL_HB_DIR": self.tmp / "hb_local",
            "BH_DIR": self.tmp / "bus_health", "TOMAIN": self.tmp / "to_main",
        }
        for name, value in roots.items():
            self.enter(patch.object(comms, name, value))
        self.enter(patch.object(agent_base, "AGENTS_ROOT", roots["AGENTS_ROOT"]))
        self.enter(patch.object(agent_base, "OPERATING_STANDARDS_PATH", self.tmp / "absent.md"))
        # Observable heartbeat that still writes the real files.
        self.heartbeat = Mock(side_effect=comms.heartbeat)
        self.enter(patch.object(agent_base, "heartbeat", self.heartbeat))
        # No real process exit, ever.
        self.exit = Mock(side_effect=SimulatedExit)
        self.enter(patch.object(agent_base, "os", types.SimpleNamespace(_exit=self.exit, getpid=os.getpid)))
        self.agents = []
        self.agent = self.make_agent()

    def enter(self, cm):
        value = cm.__enter__()
        self.addCleanup(cm.__exit__, None, None, None)
        return value

    def make_agent(self, cls=Worker):
        agent = cls()
        self.agents.append(agent)
        self.addCleanup(self.release, agent)
        return agent

    @staticmethod
    def release(agent):
        fh = getattr(agent, "_consumer_lock", None)
        if fh is not None:
            fh.close()
            agent._consumer_lock = None

    # -- helpers ---------------------------------------------------------
    def inbox(self):
        return comms.inbox_dir(self.agent.name)

    def queue(self, eid="001", caller="client", **fields):
        request = {"event_id": eid, "kind": "work", "caller": caller, **fields}
        path = self.inbox() / f"{eid}.json"
        path.write_text(json.dumps(request))
        return path

    def response(self, eid="001", caller="client"):
        return json.loads((comms.outbox_dir(caller) / f"{eid}.json").read_text())

    def outcome(self, eid="001"):
        return json.loads((self.agent.self_dir / "state" / "outcomes" / f"{eid}.json").read_text())

    def health(self):
        return json.loads((self.agent.self_dir / "state" / "health.json").read_text())

    def events(self, name=None):
        out = []
        for f in sorted((self.tmp / "audit").glob("*.jsonl")):
            for line in f.read_text().splitlines():
                rec = json.loads(line)
                if name is None or rec["event"] == name:
                    out.append(rec)
        return out

    def quarantine(self):
        return sorted(p.name for p in (self.agent.self_dir / "quarantine").iterdir())


class ReviewerContracts(BusTestCase):
    """The 21 checks from review/agent_bus_regression_tests.py, against the real code."""

    def test_01_success_replies_acknowledges_heartbeats(self):
        request = self.queue()
        self.agent.tick()
        self.assertIs(self.response()["ok"], True)
        self.assertFalse(request.exists())
        self.heartbeat.assert_called_once_with("worker")
        self.assertEqual(self.agent.idle_calls, 1)
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertEqual(self.health()["tick_seq"], 1)

    def test_02_subclass_exception_becomes_error_reply_with_traceback(self):
        request = self.queue()
        self.agent.raise_exc = RuntimeError("boom")
        self.agent.tick()
        self.assertIs(self.response()["ok"], False)
        self.assertFalse(request.exists())
        self.assertEqual(self.agent._consec_failures, 1)
        self.heartbeat.assert_called_once()
        (rec,) = self.events("handler_exception")
        self.assertEqual(rec["eid"], "001")
        self.assertEqual(rec["exception_type"], "RuntimeError")
        self.assertIn("Traceback", rec["traceback"])

    def test_03_open_breaker_heartbeats_without_work(self):
        request = self.queue()
        self.agent._circuit_breaker_pause_until = time.monotonic() + 1000
        self.agent._breaker_state = "open"
        self.agent.tick()
        self.assertEqual(self.agent.calls, [])
        self.assertTrue(request.exists())
        self.heartbeat.assert_called_once()
        self.assertEqual(self.agent.idle_calls, 0, "consequential idle work is paused while open")
        self.assertEqual(self.health()["breaker"]["state"], "open")

    def test_04_idle_exception_still_heartbeats(self):
        def bad_idle():
            raise RuntimeError("idle failed")
        self.agent.idle_cycle = bad_idle
        self.agent.tick()
        self.heartbeat.assert_called_once()
        self.assertTrue(self.events("idle_cycle_exception"))

    def test_05_plan_dispatch_uses_keywords_and_validates_arguments(self):
        self.agent.plan = Mock(return_value=self.agent.self_dir / "plans" / "p.md")
        self.queue(eid="000", kind="plan", title="Deploy", steps=["test", "release"], rationale="safety")
        self.queue(eid="001", kind="plan", title="Bad", steps="not-a-list")
        self.agent.tick()
        self.agent.plan.assert_called_once_with(title="Deploy", steps=["test", "release"], rationale="safety")
        rejected = self.response("001")
        self.assertIs(rejected["ok"], False)
        self.assertEqual(rejected["failure_class"], "rejection")
        self.assertEqual(self.agent._consec_failures, 0, "a bad request is not an agent failure")

    def test_05b_real_plan_writes_steps_in_order_and_sanitises_title(self):
        self.queue(kind="plan", title="Deploy/../x", steps=["a", "b"], rationale="why")
        self.agent.tick()
        path = Path(self.response()["plan_path"])
        self.assertEqual(path.parent, self.agent.self_dir / "plans")
        text = path.read_text()
        self.assertIn("1. a\n2. b", text)
        self.assertIn("## Rationale\n\nwhy", text)

    def test_06_breaker_stops_batch_at_threshold_after_acknowledging(self):
        for n in range(8):
            self.queue(eid=f"{n:03}")
        self.agent.result = {"ok": False, "error": "service unavailable"}
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 5)
        self.assertEqual(len(list(self.inbox().glob("*.json"))), 3)
        self.assertEqual(self.outcome("004")["status"], "acknowledged")
        self.assertIs(self.response("004")["ok"], False)
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertTrue((self.agent.self_dir / "state" / "breaker.json").exists())
        self.assertEqual(self.agent.idle_calls, 0)

    def test_07_non_object_json_is_quarantined_and_later_work_runs(self):
        (self.inbox() / "000.json").write_text("[]")
        good = self.queue()
        self.agent.tick()
        self.assertFalse(good.exists())
        self.assertEqual(len(self.agent.calls), 1)
        self.heartbeat.assert_called_once()
        self.assertEqual(sorted(p.name for p in self.inbox().glob("*.json")), [])
        self.assertIn("000.json", self.quarantine())
        reason = json.loads((self.agent.self_dir / "quarantine" / "000.json.reason.json").read_text())
        self.assertEqual(reason["reason_code"], "not_an_object")

    def test_08_builtin_exception_is_contained(self):
        self.queue(eid="000", kind="dream")
        self.queue()
        self.agent.dream_sequence = Mock(side_effect=RuntimeError("dream failed"))
        self.agent.tick()
        self.assertIs(self.response("000")["ok"], False)
        self.assertEqual(len(self.agent.calls), 1)
        self.heartbeat.assert_called_once()

    def test_09_invalid_skill_arguments_are_rejected(self):
        self.agent.invoke_skill = Mock(return_value={"ok": True})
        self.queue(eid="000", kind="invoke_skill", skill="example", args=None)
        self.queue()
        self.agent.tick()
        self.assertIs(self.response("000")["ok"], False)
        self.assertEqual(self.response("000")["failure_class"], "rejection")
        self.agent.invoke_skill.assert_not_called()
        self.assertEqual(len(self.agent.calls), 1)

    def test_10_non_dict_result_is_contained(self):
        self.queue(eid="000")
        self.queue()
        results = iter([None, {"ok": True}])
        self.agent.handle = lambda req: next(results)
        self.agent.tick()
        self.assertIs(self.response("000")["ok"], False)
        self.assertIs(self.response()["ok"], True)

    def test_11_ok_must_be_boolean(self):
        self.queue()
        self.agent.result = {"ok": "false"}
        self.agent.tick()
        reply = self.response()
        self.assertIs(reply["ok"], False)
        self.assertIn("must be a boolean", reply["error"])

    def test_12_response_write_failure_does_not_repeat_handler(self):
        self.queue()
        with patch.object(agent_base, "respond", Mock(side_effect=OSError("disk full"))):
            self.agent.tick()
        self.assertEqual(self.outcome()["status"], "recorded")
        self.assertEqual(self.outcome()["attempts"]["deliver"], 1)
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertIs(self.response()["ok"], True)
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertTrue(self.events("response_delivery_failed"))

    def test_13_unlink_failure_does_not_repeat_handler(self):
        request = self.queue()
        original_unlink = Path.unlink

        def injected(path, *a, **kw):
            if path == request:
                raise PermissionError("injected")
            return original_unlink(path, *a, **kw)
        with patch.object(Path, "unlink", new=injected):
            self.agent.tick()
        self.assertEqual(self.outcome()["status"], "delivered")
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertFalse(request.exists())
        self.assertEqual(self.outcome()["status"], "acknowledged")

    def test_14_heartbeat_attempted_after_response_io_failure(self):
        self.queue()
        with patch.object(agent_base, "respond", Mock(side_effect=OSError("disk full"))):
            self.agent.tick()
        self.heartbeat.assert_called_once()

    def test_15_respawn_is_acknowledged_then_exits_then_reconciles_on_boot(self):
        request = self.queue(kind="heal", action="respawn")
        with self.assertRaises(SimulatedExit):
            self.agent.tick()
        self.assertFalse(request.exists())
        reply = self.response()
        self.assertIs(reply["ok"], True)
        self.assertTrue(reply["deferred"])
        self.heartbeat.assert_called_once()
        intent_path = self.agent.self_dir / "state" / "restart_intent.json"
        self.assertEqual(json.loads(intent_path.read_text())["status"], "exiting")
        # Supervisor restarts the daemon: the intent is archived, not executed.
        self.release(self.agent)
        self.agent = self.make_agent()
        self.assertEqual(json.loads(intent_path.read_text())["status"], "completed")
        self.assertTrue(self.events("restart_intent_reconciled"))
        self.exit.reset_mock()
        self.agent.tick()
        self.exit.assert_not_called()

    def test_16_result_cannot_override_transport_event_id(self):
        self.queue()
        self.agent.result = {"ok": True, "event_id": "different-request"}
        self.agent.tick()
        self.assertEqual(self.response()["event_id"], "001")

    def test_17_caller_cannot_escape_agents_root(self):
        with self.assertRaises(ValueError):
            comms.respond("../escaped", "001", {"ok": True})
        self.assertFalse((self.tmp / "escaped").exists())
        self.assertFalse((self.tmp / "agents" / ".." / "escaped").exists())

    def test_18_event_id_cannot_escape_outbox(self):
        with self.assertRaises(ValueError):
            comms.respond("client", "../escaped", {"ok": True})
        self.assertFalse((self.tmp / "agents" / "client" / "escaped.json").exists())
        self.assertFalse((self.tmp / "agents" / "client" / "outbox" / "..").exists())

    def test_19_error_object_still_trips_breaker(self):
        self.agent._consec_failures = 4
        self.queue()
        self.agent.result = {"ok": False, "error": {"code": "down"}}
        self.agent.tick()
        self.assertTrue(self.agent._circuit_breaker_open())
        self.heartbeat.assert_called_once()
        self.assertEqual(self.response()["error"], "{'code': 'down'}")

    def test_20_audit_failure_does_not_terminate_run_loop(self):
        self.agent.tick = Mock(side_effect=RuntimeError("work failed"))
        fake_time = types.SimpleNamespace(time=time.time, monotonic=time.monotonic,
                                          sleep=Mock(side_effect=SimulatedExit))
        with patch.object(agent_base, "audit", Mock(side_effect=OSError("audit volume unavailable"))), \
                patch.object(agent_base, "time", fake_time):
            with self.assertRaises(SimulatedExit):
                self.agent.run()
        fallback = (self.agent.self_dir / "logs" / "audit_fallback.jsonl").read_text()
        self.assertIn('"event": "tick_exception"', fallback)
        self.assertIn("RuntimeError", fallback)
        self.assertTrue(self.agent._audit_degraded)

    def test_21_malformed_json_is_preserved_byte_for_byte(self):
        original = "not valid json"
        (self.inbox() / "000.json").write_text(original)
        self.agent.tick()
        kept = self.agent.self_dir / "quarantine" / "000.json"
        self.assertEqual(kept.read_text(), original)
        self.assertEqual(list(self.inbox().glob("*.json")), [])
        self.assertEqual(json.loads((kept.parent / "000.json.reason.json").read_text())["reason_code"], "decode_error")


class LifecycleContracts(BusTestCase):
    """Guarantees beyond the reviewer's 21 checks."""

    def test_crash_between_side_effect_and_record_is_uncertain_not_rerun(self):
        request = self.queue()
        self.agent.raise_exc = KeyboardInterrupt()  # process dies inside the handler
        with self.assertRaises(KeyboardInterrupt):
            self.agent.tick()
        self.assertEqual(self.outcome()["status"], "executing")
        self.assertTrue(request.exists())
        self.agent.raise_exc = None
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1, "handler must not run again")
        reply = self.response()
        self.assertIs(reply["ok"], False)
        self.assertEqual(reply["outcome"], "uncertain")
        self.assertEqual((reply["event_id"], reply["kind"], reply["status"]), ("001", "work", "uncertain"))
        self.assertFalse(request.exists())
        self.assertEqual(self.outcome()["status"], "acknowledged")
        self.assertTrue(list((self.tmp / "to_main").glob("*OUTCOME_UNCERTAIN*")))
        self.assertEqual(self.agent._consec_failures, 0, "uncertain is not a dependency failure")

    def test_crash_recovery_reexecutes_only_idempotent_kinds(self):
        self.agent.IDEMPOTENT_KINDS = ("work",)
        self.queue()
        self.agent.raise_exc = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.agent.tick()
        self.agent.raise_exc = None
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 2)
        self.assertIs(self.response()["ok"], True)
        self.assertEqual(self.outcome()["attempts"]["execute"], 2)
        self.assertTrue(self.events("reexecuting_idempotent"))

    def test_duplicate_same_input_returns_recorded_result(self):
        self.queue(payload=1)
        self.agent.tick()
        (comms.outbox_dir("client") / "001.json").unlink()
        self.queue(payload=1)
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        self.assertEqual(self.response()["value"], "done")
        self.assertTrue(self.events("duplicate_replayed"))
        self.assertEqual(list(self.inbox().glob("*.json")), [])

    def test_duplicate_different_input_is_rejected(self):
        self.queue(payload=1)
        self.agent.tick()
        (comms.outbox_dir("client") / "001.json").unlink()
        self.queue(payload=2)
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)
        reply = self.response()
        self.assertIs(reply["ok"], False)
        self.assertEqual(reply["failure_class"], "rejection")
        self.assertEqual(self.outcome()["result"]["value"], "done", "original record untouched")
        self.assertTrue(self.events("identity_reuse"))
        self.assertEqual(self.agent._consec_failures, 0)

    def test_overlapping_consumer_is_refused(self):
        with self.assertRaises(RuntimeError):
            Worker()
        self.release(self.agent)
        second = self.make_agent()
        self.assertIsNotNone(second._consumer_lock)
        self.assertTrue((second.self_dir / "state" / "consumer.owner.json").exists())

    def test_overlapping_consumer_cannot_double_execute_claimed_message(self):
        # Even without the lock, a message whose record says executing is not run twice.
        self.queue()
        self.agent.raise_exc = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.agent.tick()
        self.release(self.agent)
        other = self.make_agent()
        other.tick()
        self.assertEqual(other.calls, [])
        self.assertEqual(self.response()["outcome"], "uncertain")

    def test_executing_record_write_failure_stops_before_executing(self):
        self.queue(eid="000")
        self.queue(eid="001")
        with patch.object(self.agent, "_save_outcome", return_value=False):
            self.agent.tick()
        self.assertEqual(self.agent.calls, [])
        self.assertEqual(len(list(self.inbox().glob("*.json"))), 2)
        self.assertEqual(self.agent._consec_failures, 1)
        self.heartbeat.assert_called_once()

    def test_transient_read_error_is_left_in_place_not_quarantined(self):
        request = self.queue()
        original = os.open   # read_message opens the entry itself (O_NOFOLLOW) and reads via the fd

        def flaky(path, *a, **kw):
            if Path(path) == request:
                raise OSError("EIO")
            return original(path, *a, **kw)
        with patch.object(os, "open", new=flaky):
            self.agent.tick()
        self.assertTrue(request.exists())
        self.assertEqual(self.quarantine(), [])
        self.assertTrue(self.events("inbox_read_error"))
        self.assertEqual(self.agent.calls, [])
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 1)

    def test_envelope_mismatch_and_bad_caller_are_quarantined(self):
        (self.inbox() / "000.json").write_text(json.dumps({"event_id": "other", "kind": "work", "caller": "client"}))
        (self.inbox() / "001.json").write_text(json.dumps({"event_id": "001", "kind": "work", "caller": "../x"}))
        (self.inbox() / "002.json").write_text(json.dumps({"event_id": "002", "kind": 5, "caller": "client"}))
        good = self.queue(eid="003")
        self.agent.tick()
        self.assertEqual(list(self.inbox().glob("*.json")), [])
        self.assertFalse(good.exists())
        codes = {json.loads((self.agent.self_dir / "quarantine" / f"{n}.json.reason.json").read_text())["reason_code"]
                 for n in ("000", "001", "002")}
        self.assertEqual(codes, {"event_id_mismatch", "bad_identifier", "bad_kind"})
        self.assertEqual(self.health()["quarantined_total"], 3)

    def test_missing_kind_still_reaches_handle(self):
        (self.inbox() / "001.json").write_text(json.dumps({"event_id": "001", "caller": "client", "request": "get_token"}))
        self.agent.tick()
        self.assertEqual(self.agent.calls[0]["request"], "get_token")
        self.assertIs(self.response()["ok"], True)

    def test_breaker_half_open_probe_reopens_with_doubled_cooldown_then_closes(self):
        self.agent._consec_failures = 4
        self.queue(eid="000")
        self.agent.result = {"ok": False, "error": "down"}
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertEqual(self.agent._breaker_cooldown_s, 300)
        # Cooldown passes; one probe is admitted and fails.
        self.agent._circuit_breaker_pause_until = time.monotonic() - 1
        self.queue(eid="001")
        self.queue(eid="002")
        self.agent.tick()
        self.assertEqual([r["event_id"] for r in self.agent.calls], ["000", "001"], "exactly one probe")
        self.assertEqual(self.agent._breaker_state, "open")
        self.assertEqual(self.agent._breaker_cooldown_s, 600)
        self.assertTrue((self.inbox() / "002.json").exists())
        # Cooldown passes again; the probe succeeds and the breaker closes.
        self.agent._circuit_breaker_pause_until = time.monotonic() - 1
        self.agent.result = {"ok": True}
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "closed")
        self.assertEqual(self.agent._consec_failures, 0)
        self.assertEqual(self.agent._breaker_cooldown_s, 300)
        self.assertTrue(self.events("circuit_breaker_half_open"))
        self.assertTrue(self.events("circuit_breaker_closed"))

    def test_rejections_do_not_count_toward_breaker(self):
        for n in range(6):
            self.queue(eid=f"{n:03}")
        self.agent.result = {"ok": False, "error": "bad request", "failure_class": "rejection"}
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 6)
        self.assertEqual(self.agent._consec_failures, 0)
        self.assertFalse(self.agent._circuit_breaker_open())

    def test_heal_is_delivered_while_breaker_open(self):
        self.agent._circuit_breaker_pause_until = time.monotonic() + 1000
        self.agent._breaker_state = "open"
        work = self.queue(eid="000")
        heal = self.queue(eid="001", kind="heal", action="rewrite_north_star")
        self.agent.tick()
        self.assertTrue(work.exists())
        self.assertFalse(heal.exists())
        self.assertIs(self.response("001")["ok"], True)
        self.assertTrue((self.agent.self_dir / "NORTH_STAR.md").exists())
        self.assertEqual(self.agent.calls, [])

    def test_trip_breaker_alert_has_real_newlines(self):
        self.agent._trip_breaker("test reason")
        (alert,) = list((self.tmp / "to_main").glob("*CIRCUIT_BREAKER_TRIPPED*"))
        body = alert.read_text()
        self.assertIn("\n\n**reason**: test reason\n", body)
        self.assertNotIn("\\n", body)

    def test_soft_reset_removes_only_stale_tmp_files(self):
        fresh = self.inbox() / "fresh.json.tmp"
        stale = comms.outbox_dir(self.agent.name) / "stale.json.tmp"
        fresh.write_text("{}")
        stale.write_text("{}")
        old = time.time() - 3600
        os.utime(stale, (old, old))
        self.queue(kind="heal", action="soft_reset")
        self.agent.tick()
        self.assertTrue(fresh.exists())
        self.assertFalse(stale.exists())
        self.assertEqual(self.response()["removed"], 1)
        (rec,) = self.events("self_heal_tmp_removed")
        self.assertGreaterEqual(rec["age_s"], 3599)

    def test_unknown_heal_action_is_a_rejection(self):
        self.queue(kind="heal", action="explode")
        self.agent.tick()
        self.assertEqual(self.response()["failure_class"], "rejection")
        self.assertEqual(self.agent._consec_failures, 0)

    def test_privileged_kinds_require_hmac_when_key_exists(self):
        self.queue(eid="000", kind="heal", action="rewrite_north_star")
        self.agent.tick()
        self.assertIs(self.response("000")["ok"], True)
        self.assertTrue(self.events("privileged_unauthenticated"))
        self.assertFalse(self.health()["privileged_auth_enforced"])
        key = b"secret-key"
        trust = self.tmp / "agents" / "_trust"
        trust.mkdir()
        (trust / "operator.key").write_bytes(key)
        self.queue(eid="001", kind="heal", action="soft_reset")
        self.queue(eid="002", kind="heal", action="soft_reset",
                   auth=comms.sign_directive(key, "002", "heal", "soft_reset", "client", target="worker"))
        self.agent.tick()
        self.assertIs(self.response("001")["ok"], False)
        self.assertEqual(self.response("001")["failure_class"], "rejection")
        self.assertIs(self.response("002")["ok"], True)
        self.assertTrue(self.health()["privileged_auth_enforced"])

    def test_heartbeat_once_on_every_path(self):
        (self.inbox() / "000.json").write_text("garbage")
        self.queue(eid="001")
        self.agent.raise_exc = RuntimeError("x")
        with patch.object(agent_base, "respond", Mock(side_effect=OSError("disk full"))):
            self.agent.tick()
        self.heartbeat.assert_called_once_with("worker")
        self.assertEqual(self.health()["heartbeat_written"], [True, True])

    def test_comms_audit_falls_back_and_never_raises(self):
        blocker = self.tmp / "audit-blocker"
        blocker.write_text("not a directory")
        with patch.object(comms, "AUDIT_DIR", blocker):
            self.assertFalse(comms.audit("worker", "probe", n=1))
        fallback = list((self.tmp / "agents" / "_audit_fallback").glob("*_worker.jsonl"))
        self.assertEqual(len(fallback), 1)
        rec = json.loads(fallback[0].read_text().splitlines()[-1])
        self.assertEqual(rec["event"], "probe")
        self.assertIn("audit_primary_error", rec)

    def test_maybe_dream_at_minute_58_uses_timedelta_and_persists_slot(self):
        self.agent.dream_times_utc = (dtime(23, 58),)
        self.agent.dream_sequence = Mock()
        now = datetime(2026, 9, 24, 23, 59, 30, tzinfo=timezone.utc)
        self.agent._maybe_dream(now_utc=now)
        self.agent.dream_sequence.assert_called_once()
        slots = json.loads((self.agent.self_dir / "state" / "dream_slots.json").read_text())
        self.assertEqual(slots["2026-09-24T23:58"]["status"], "completed")
        # A restart inside the window does not run it again.
        self.release(self.agent)
        again = self.make_agent()
        again.dream_times_utc = (dtime(23, 58),)
        again.dream_sequence = Mock()
        again._maybe_dream(now_utc=now)
        again.dream_sequence.assert_not_called()

    def test_maybe_dream_missed_slot_catch_up_and_retry_limit(self):
        self.agent.dream_times_utc = (dtime(6, 30),)
        self.agent.dream_sequence = Mock(side_effect=RuntimeError("dream broke"))
        late = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)
        for _ in range(5):
            self.agent._maybe_dream(now_utc=late)
        self.assertEqual(self.agent.dream_sequence.call_count, 3, "at most three attempts")
        slots = json.loads((self.agent.self_dir / "state" / "dream_slots.json").read_text())
        self.assertEqual(slots["2026-09-24T06:30"]["status"], "failed")
        self.assertTrue(slots["2026-09-24T06:30"]["late"])
        self.agent.dream_catch_up = False
        self.agent.dream_sequence = Mock()
        self.agent._maybe_dream(now_utc=late.replace(day=25))
        self.agent.dream_sequence.assert_not_called()
        self.assertTrue(self.events("missed_dream"))

    def test_per_tick_message_budget_leaves_remainder_then_runs_maintenance(self):
        self.agent.max_messages_per_tick = 2
        for n in range(5):
            self.queue(eid=f"{n:03}")
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 2)
        self.assertEqual(len(list(self.inbox().glob("*.json"))), 3)
        self.assertEqual(self.agent.idle_calls, 1)
        self.heartbeat.assert_called_once()
        (rec,) = self.events("tick_budget_exhausted")
        self.assertEqual((rec["processed"], rec["remaining"]), (2, 3))

    def test_per_tick_time_budget_stops_draining(self):
        self.agent.tick_budget_s = 1e-9
        for n in range(3):
            self.queue(eid=f"{n:03}")
        self.agent.tick()
        self.assertEqual(len(self.agent.calls), 0)
        self.assertEqual(len(list(self.inbox().glob("*.json"))), 3)
        self.assertEqual(self.agent.idle_calls, 1)

    def test_health_json_reports_the_four_signals(self):
        self.queue()
        self.agent.tick()
        h = self.health()
        for key in ("alive", "tick_seq", "last_progress_ts", "breaker", "current_op", "inbox_pending",
                    "quarantined_total", "audit_degraded", "heartbeat_written", "privileged_auth_enforced", "bd"):
            self.assertIn(key, h)
        self.assertIsNotNone(h["last_progress_ts"])
        self.assertIsNone(h["current_op"])
        self.assertEqual(h["bd"], "stub")
        self.assertFalse((self.agent.self_dir / "state" / "current_op.json").exists())

    def test_current_op_file_exists_while_handler_runs(self):
        seen = {}

        def observe(req):
            seen["op"] = json.loads((self.agent.self_dir / "state" / "current_op.json").read_text())
            return {"ok": True}
        self.agent.handle = observe
        self.queue()
        self.agent.tick()
        self.assertEqual(seen["op"]["event_id"], "001")

    def test_bd_stub_is_reported_and_never_called(self):
        self.assertEqual(agent_base.BD_STATUS, "stub")
        self.assertTrue(self.agent._bd_degraded)
        self.assertTrue(self.events("bd_unavailable"))
        self.agent.BEAD_WORTHY_KINDS = ("work",)
        with patch.object(agent_base, "bd_create", Mock(side_effect=AssertionError("must not be called"))):
            self.queue()
            self.agent.tick()
        self.assertIs(self.response()["ok"], True)

    def test_message_exception_is_audited_and_next_message_runs(self):
        self.queue(eid="000")
        self.queue(eid="001")
        original = self.agent._load_outcome

        def broken(eid):
            if eid == "000":
                raise OSError("state volume flaked")
            return original(eid)
        with patch.object(self.agent, "_load_outcome", side_effect=broken):
            self.agent.tick()
        self.assertTrue((self.inbox() / "000.json").exists())
        self.assertFalse((self.inbox() / "001.json").exists())
        (rec,) = self.events("message_exception")
        self.assertEqual(rec["eid"], "000")


class CommsContracts(BusTestCase):
    def test_validate_identifier_rules(self):
        for good in ("review-agent", "client", "evt-1a2b.3c", "A_b-9"):
            self.assertEqual(comms.validate_identifier(good), good)
        for bad in ("", ".", "..", "../x", "a/b", "a\\b", ".hidden", "x" * 129, None, 5, "sp ace"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                comms.validate_identifier(bad)

    def test_dirs_validate_before_mkdir(self):
        with self.assertRaises(ValueError):
            comms.inbox_dir("../up")
        with self.assertRaises(ValueError):
            comms.outbox_dir("a/b")
        self.assertFalse((self.tmp / "up").exists())
        self.assertFalse((self.tmp / "agents" / "a").exists())

    def test_respond_tmp_name_keeps_dotted_ids_in_outbox(self):
        comms.respond("client", "evt.1.2", {"ok": True})
        out = comms.outbox_dir("client")
        self.assertTrue((out / "evt.1.2.json").exists())
        self.assertEqual([p.name for p in out.iterdir()], ["evt.1.2.json"])

    def test_respond_rejects_non_dict_payload(self):
        with self.assertRaises(TypeError):
            comms.respond("client", "001", None)

    def test_drain_inbox_never_unlinks(self):
        (self.inbox() / "000.json").write_text("{bad")
        self.queue(eid="001")
        drained = list(comms.drain_inbox("worker"))
        self.assertEqual([p.name for p, _ in drained], ["001.json"])
        self.assertTrue((self.inbox() / "001.json").exists())
        self.assertIn("000.json", self.quarantine())

    def test_quarantine_collision_keeps_both_files(self):
        (self.inbox() / "000.json").write_text("first")
        list(comms.drain_inbox("worker"))
        (self.inbox() / "000.json").write_text("second")
        list(comms.drain_inbox("worker"))
        contents = sorted(p.read_text() for p in (self.agent.self_dir / "quarantine").glob("000.json*")
                          if not p.name.endswith(".reason.json"))
        self.assertEqual(contents, ["first", "second"])

    def test_oversized_message_is_quarantined(self):
        (self.inbox() / "000.json").write_text("[" + "1," * 600000 + "1]")
        list(comms.drain_inbox("worker"))
        reason = json.loads((self.agent.self_dir / "quarantine" / "000.json.reason.json").read_text())
        self.assertEqual(reason["reason_code"], "oversized")

    def test_send_directive_validates_and_honours_registry(self):
        with self.assertRaises(ValueError):
            comms.send_directive("../x", {"kind": "work"}, caller="client", timeout_s=0)
        (self.tmp / "agents" / "_registry.json").write_text(json.dumps(["worker"]))
        with self.assertRaises(ValueError):
            comms.send_directive("ghost", {"kind": "work"}, caller="client", timeout_s=0)
        self.assertIsNone(comms.send_directive("worker", {"kind": "work", "event_id": "d1"}, caller="client", timeout_s=0))
        self.assertTrue((self.inbox() / "d1.json").exists())

    def test_heartbeat_reports_what_it_wrote(self):
        self.assertEqual(comms.heartbeat("worker"), (True, True))
        with patch.object(comms, "HB_DIR", self.tmp / "audit-blocker-file"):
            (self.tmp / "audit-blocker-file").write_text("x")
            self.assertEqual(comms.heartbeat("worker"), (True, False))

    def test_validate_result_normalises(self):
        v = self.agent._validate_result
        self.assertEqual(v(None)[1], "failure")
        self.assertEqual(v({"ok": "false"})[0]["ok"], False)
        self.assertEqual(v({"ok": True, "event_id": "x"})[0], {"ok": True})
        self.assertEqual(v({"ok": False, "error": {"a": 1}})[0]["error"], "{'a': 1}")
        self.assertEqual(v({"ok": False, "failure_class": "rejection"})[1], "rejection")
        self.assertEqual(v({"ok": False, "failure_class": "made-up"})[1], "failure")

    def test_validate_directive_reports_actual_kind(self):
        ok, msg = self.agent.validate_directive({"kind": "nope"}, kinds=("work",))
        self.assertFalse(ok)
        self.assertEqual(msg, "unsupported kind 'nope'")


if __name__ == "__main__":
    unittest.main(verbosity=2)

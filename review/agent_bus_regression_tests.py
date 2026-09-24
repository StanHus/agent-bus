#!/usr/bin/env python3
"""Isolated behavioral regression checks for StanHus/agent-bus at a4a7bbc.

Usage (Python 3.10+; standard library only):
    python agent_bus_regression_tests.py --repo /path/to/agent-bus

Tests assert DESIRED reliability behavior. Failures on a4a7bbc are intentional
findings, not a claim that the test run or repository is healthy. Four baseline
checks confirm the harness exercises the normal contract correctly.

The loader compiles selected original methods/functions using AST. It does not
import private integrations, call BaseAgent.__init__, run real respawns, or touch
production paths. Everything written by a test stays in a temporary directory.
This is not an integration, deployment, or full crash-recovery test suite.
"""
from __future__ import annotations

import argparse
import ast
import json
import inspect
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from unittest.mock import Mock, patch

REPO: Path


def functions_from(path: Path, names: set[str], class_name: str | None = None):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    body = tree.body
    if class_name is not None:
        classes = [n for n in body if isinstance(n, ast.ClassDef) and n.name == class_name]
        if not classes:
            raise ValueError(f"{path}: class {class_name!r} was not found")
        body = classes[0].body
    functions = [n for n in body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    missing = names - {n.name for n in functions}
    if missing:
        raise ValueError(f"{path}: required functions missing: {sorted(missing)}")
    return functions


class SimulatedExit(BaseException):
    """Used instead of actually terminating the test process."""


class AgentBusContractTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.audit = Mock(name="audit")
        self.heartbeat = Mock(name="heartbeat")
        self.alert = Mock(name="alert_main")
        # Prevent real exits and real sleeping, without modifying os/time globally.
        self.safe_os = types.SimpleNamespace(_exit=Mock(side_effect=SimulatedExit))
        self.safe_time = types.SimpleNamespace(time=time.time, monotonic=time.monotonic, sleep=Mock())
        common = {"json": json, "Path": Path, "datetime": datetime,
                  "timezone": timezone, "os": self.safe_os, "time": self.safe_time}
        self.comms = types.ModuleType("comms")
        self.comms.__dict__.update(common)
        self.comms.AGENTS_ROOT = self.root / "agents"
        self.comms.AUDIT_DIR = self.root / "audit"
        comms_functions = functions_from(
            REPO / "comms.py", {"now_iso", "audit", "inbox_dir", "outbox_dir", "drain_inbox", "respond",
                                "validate_identifier", "read_message", "quarantine_message",
                                "validate_agent_name", "agent_subdir", "known_agents", "signed_message"}
        )
        exec(compile(ast.Module(body=comms_functions, type_ignores=[]), str(REPO / "comms.py"), "exec"), self.comms.__dict__)
        self.original_audit = self.comms.audit
        self.comms.audit = self.audit
        self.comms.alert_main = self.alert
        self.stack.enter_context(patch.dict(sys.modules, {"comms": self.comms}))
        methods = functions_from(
            REPO / "agent_base.py",
            {"tick", "run", "_circuit_breaker_open", "_trip_breaker", "healing_handler",
             "_audit_safe", "_write_health", "_dispatch", "_validate_result", "_load_outcome",
             "_save_outcome", "_process_message", "_controlled_exit", "_reconcile_restart_intent",
             "_safe_str", "_count_failure", "_quarantined_count", "_operator_key", "_claim_is_live"},
            "BaseAgent",
        )
        self.env = dict(common)
        self.env.update(audit=self.audit, heartbeat=self.heartbeat, respond=self.comms.respond,
                        drain_inbox=self.comms.drain_inbox, now_iso=self.comms.now_iso)
        # Compiled as free functions, then bound to a minimal test class.
        exec(compile(ast.Module(body=methods, type_ignores=[]), str(REPO / "agent_base.py"), "exec"), self.env)
        cls = type("ExtractedBaseAgent", (), {method.name: self.env[method.name] for method in methods})
        self.agent = cls()
        self.agent.name = "review-agent"
        self.agent.cadence_s = 0
        self.agent.BEAD_WORTHY_KINDS = ()
        self.agent.self_dir = self.comms.AGENTS_ROOT / self.agent.name
        for sub in ("inbox", "outbox", "state", "plans", "learnings"):
            (self.agent.self_dir / sub).mkdir(parents=True, exist_ok=True)
        self.agent._consec_failures = 0
        self.agent._circuit_breaker_threshold = 5
        self.agent._circuit_breaker_pause_until = 0
        self.agent._bd_degraded = False
        self.agent.handle = Mock(return_value={"ok": True, "value": "done"})
        self.agent.idle_cycle = Mock()
        self.agent._maybe_dream = Mock()
        self.agent.dream_sequence = Mock()
        self.agent.invoke_skill = Mock(return_value={"ok": True})
        self.agent.plan = Mock(return_value=self.agent.self_dir / "plans" / "test.md")
        self.agent._bd_trace_create = Mock(return_value=None)
        self.agent._bd_trace_close = Mock(return_value=True)

    def queue(self, eid="001", **fields):
        request = {"event_id": eid, "kind": "work", "caller": "client", **fields}
        path = self.comms.inbox_dir(self.agent.name) / f"{eid}.json"
        path.write_text(json.dumps(request), encoding="utf-8")
        return path

    def response(self, eid="001"):
        return json.loads((self.comms.outbox_dir("client") / f"{eid}.json").read_text(encoding="utf-8"))

    def test_baseline_success_replies_acknowledges_and_heartbeats(self):
        request = self.queue()
        self.agent.tick()
        self.assertTrue(self.response()["ok"])
        self.assertFalse(request.exists())
        self.heartbeat.assert_called_once_with(self.agent.name)
        self.agent.idle_cycle.assert_called_once()

    def test_baseline_subclass_exception_becomes_error_reply(self):
        request = self.queue()
        self.agent.handle.side_effect = RuntimeError("boom")
        self.agent.tick()
        self.assertIs(self.response()["ok"], False)
        self.assertFalse(request.exists())
        self.assertEqual(self.agent._consec_failures, 1)
        self.heartbeat.assert_called_once()

    def test_baseline_open_breaker_heartbeats_without_work(self):
        request = self.queue()
        self.agent._circuit_breaker_pause_until = time.monotonic() + 1000
        self.agent.tick()
        self.agent.handle.assert_not_called()
        self.assertTrue(request.exists())
        self.heartbeat.assert_called_once()

    def test_baseline_idle_exception_still_allows_heartbeat(self):
        self.agent.idle_cycle.side_effect = RuntimeError("idle failed")
        self.agent.tick()
        self.heartbeat.assert_called_once()

    def test_plan_dispatch_preserves_named_arguments(self):
        self.queue(kind="plan", title="Deploy", steps=["test", "release"], rationale="safety")
        self.agent.tick()
        self.agent.plan.assert_called_once()
        # Signature of BaseAgent.plan at upstream lines 147-148.
        def plan_contract(title, requirements=None, steps=None, decision_gates=None,
                          failure_scenarios=None, insights=None, dependencies=None, rationale=""):
            pass
        args, kwargs = self.agent.plan.call_args
        received = inspect.signature(plan_contract).bind(*args, **kwargs)
        received.apply_defaults()
        self.assertEqual(received.arguments["steps"], ["test", "release"])
        self.assertEqual(received.arguments["rationale"], "safety")
        self.assertIsNone(received.arguments["requirements"])

    def test_breaker_stops_batch_at_threshold(self):
        for number in range(8):
            self.queue(eid=f"{number:03}")
        self.agent.handle.return_value = {"ok": False, "error": "service unavailable"}
        self.agent.tick()
        self.assertEqual(self.agent.handle.call_count, 5)
        self.assertEqual(len(list(self.comms.inbox_dir(self.agent.name).glob("*.json"))), 3)

    def test_non_object_json_does_not_block_later_work(self):
        (self.comms.inbox_dir(self.agent.name) / "000.json").write_text("[]", encoding="utf-8")
        good = self.queue()
        self.agent.tick()
        self.assertFalse(good.exists())
        self.agent.handle.assert_called_once()
        self.heartbeat.assert_called_once()

    def test_builtin_exception_does_not_block_later_work(self):
        self.queue(eid="000", kind="dream")
        self.queue()
        self.agent.dream_sequence.side_effect = RuntimeError("dream failed")
        self.agent.tick()
        self.assertIs(self.response("000")["ok"], False)
        self.agent.handle.assert_called_once()
        self.heartbeat.assert_called_once()

    def test_invalid_skill_arguments_are_rejected_without_aborting_tick(self):
        self.queue(eid="000", kind="invoke_skill", skill="example", args=None)
        self.queue()
        self.agent.tick()
        self.assertIs(self.response("000")["ok"], False)
        self.agent.handle.assert_called_once()

    def test_invalid_handler_result_is_contained(self):
        self.queue(eid="000")
        self.queue()
        self.agent.handle.side_effect = [None, {"ok": True}]
        self.agent.tick()
        self.assertIs(self.response("000")["ok"], False)
        self.assertIs(self.response()["ok"], True)

    def test_ok_must_be_a_boolean_not_a_truthy_string(self):
        self.queue()
        self.agent.handle.return_value = {"ok": "false"}
        self.agent.tick()
        self.assertIs(self.response()["ok"], False)

    def test_response_write_failure_does_not_repeat_side_effect(self):
        self.queue()
        original = self.env["respond"]
        self.env["respond"] = Mock(side_effect=OSError("injected full disk"))
        try:
            self.agent.tick()
        except OSError:
            pass
        self.env["respond"] = original
        self.agent.tick()
        self.assertEqual(self.agent.handle.call_count, 1, "response retry repeated completed handler")

    def test_unlink_failure_does_not_repeat_side_effect(self):
        request = self.queue()
        original_unlink = Path.unlink
        def injected_unlink(path, *args, **kwargs):
            if path == request:
                raise PermissionError("injected acknowledgement failure")
            return original_unlink(path, *args, **kwargs)
        with patch.object(Path, "unlink", new=injected_unlink):
            self.agent.tick()
        self.agent.tick()
        self.assertEqual(self.agent.handle.call_count, 1, "acknowledgement retry repeated completed handler")

    def test_heartbeat_is_attempted_after_response_io_failure(self):
        self.queue()
        self.env["respond"] = Mock(side_effect=OSError("injected full disk"))
        try:
            self.agent.tick()
        except OSError:
            pass
        self.heartbeat.assert_called_once()

    def test_respawn_is_acknowledged_before_process_exit(self):
        request = self.queue(kind="heal", action="respawn")
        with self.assertRaises(SimulatedExit):
            self.agent.tick()
        self.assertFalse(request.exists(), "same respawn request remains executable after restart")

    def test_result_cannot_override_transport_event_id(self):
        self.queue()
        self.agent.handle.return_value = {"ok": True, "event_id": "different-request"}
        self.agent.tick()
        self.assertEqual(self.response()["event_id"], "001")

    def test_caller_cannot_escape_agents_root(self):
        try:
            self.comms.respond("../escaped", "001", {"ok": True})
        except (ValueError, PermissionError):
            pass
        self.assertFalse((self.root / "escaped" / "outbox" / "001.json").exists())

    def test_event_id_cannot_escape_outbox(self):
        try:
            self.comms.respond("client", "../escaped", {"ok": True})
        except (ValueError, PermissionError):
            pass
        self.assertFalse((self.comms.AGENTS_ROOT / "client" / "escaped.json").exists())

    def test_error_object_does_not_break_circuit_bookkeeping(self):
        self.agent._consec_failures = 4
        self.queue()
        self.agent.handle.return_value = {"ok": False, "error": {"code": "down"}}
        self.agent.tick()
        self.assertTrue(self.agent._circuit_breaker_open())
        self.heartbeat.assert_called_once()

    def test_audit_failure_does_not_terminate_run_loop(self):
        self.agent.tick = Mock(side_effect=RuntimeError("work failed"))
        self.env["audit"] = Mock(side_effect=OSError("audit volume unavailable"))
        self.safe_time.sleep.side_effect = SimulatedExit
        with self.assertRaises(SimulatedExit):
            self.agent.run()

    def test_malformed_json_is_preserved_for_diagnosis(self):
        original = "not valid json"
        path = self.comms.inbox_dir(self.agent.name) / "000.json"
        path.write_text(original, encoding="utf-8")
        self.agent.tick()
        preserved = []
        for candidate in self.root.rglob("*"):
            if candidate.is_file() and candidate.read_text(encoding="utf-8", errors="replace") == original:
                preserved.append(candidate)
        self.assertTrue(preserved, "malformed message was deleted instead of quarantined")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", type=Path, required=True, help="directory containing agent_base.py and comms.py")
    options, unittest_args = parser.parse_known_args()
    REPO = options.repo.resolve()
    for filename in ("agent_base.py", "comms.py"):
        if not (REPO / filename).is_file():
            parser.error(f"required source file does not exist: {REPO / filename}")
    unittest.main(argv=[sys.argv[0], *unittest_args], verbosity=2)

"""Adversarial tests, lens: transport trust boundary.

    /opt/homebrew/bin/python3 -m unittest tests.adversarial_trust -v

Every test here is written against the guarantees in CONTRACT.md ("Identifiers
are not paths", "drain_inbox never deletes / quarantines bad input", "Privileged
kinds", "bytes are exactly what arrived") from the point of view of a sender who
can only drop files into an inbox and into its own outbox. A test that fails
against the current code is a confirmed break; the module docstring of each
test says what the expected behaviour is and why.

All filesystem roots are redirected into a per-test temporary directory. Nothing
here touches the real agent tree, iCloud or the process (os._exit is mocked).
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import types
import unittest
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
        super().__init__()

    def handle(self, req):
        self.calls.append(req)
        return self.result

    def idle_cycle(self):
        pass


class TrustTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="agent-bus-trust-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        roots = {
            "AGENTS_ROOT": self.tmp / "agents", "AUDIT_DIR": self.tmp / "audit",
            "HB_DIR": self.tmp / "hb", "LOCAL_HB_DIR": self.tmp / "hb_local",
            "BH_DIR": self.tmp / "bus_health", "TOMAIN": self.tmp / "to_main",
        }
        for name, value in roots.items():
            self.enter(patch.object(comms, name, value))
        self.agents_root = roots["AGENTS_ROOT"]
        self.enter(patch.object(agent_base, "AGENTS_ROOT", self.agents_root))
        self.enter(patch.object(agent_base, "OPERATING_STANDARDS_PATH", self.tmp / "absent.md"))
        self.exit = Mock(side_effect=SimulatedExit)
        self.enter(patch.object(agent_base, "os", types.SimpleNamespace(_exit=self.exit, getpid=os.getpid)))
        self.agent = Worker()
        self.addCleanup(self.release, self.agent)

    def enter(self, cm):
        value = cm.__enter__()
        self.addCleanup(cm.__exit__, None, None, None)
        return value

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

    def quarantine_dir(self):
        return self.agent.self_dir / "quarantine"

    def quarantined_requests(self):
        return sorted(p.name for p in self.quarantine_dir().iterdir()
                      if not p.name.endswith(".reason.json"))

    def events(self, name):
        out = []
        for f in sorted((self.tmp / "audit").glob("*.jsonl")):
            for line in f.read_text().splitlines():
                rec = json.loads(line)
                if rec["event"] == name:
                    out.append(rec)
        return out


class SymlinkBoundary(TrustTestCase):
    """The agent writes as its own uid. A sender who owns only its inbox entries
    and its own outbox must not be able to point that uid at another file."""

    def test_respond_does_not_follow_a_symlinked_tmp_in_the_caller_outbox(self):
        # The caller pre-plants outbox/<eid>.json.tmp as a symlink to a file it
        # cannot write itself. respond() must create the temp file exclusively
        # (O_EXCL / O_NOFOLLOW) instead of writing through the link.
        victim = self.tmp / "victim.txt"
        victim.write_text("precious")
        out = comms.outbox_dir("client")
        (out / "001.json.tmp").symlink_to(victim)
        self.queue(eid="001", caller="client")
        self.agent.tick()
        self.assertEqual(victim.read_text(), "precious",
                         "response payload was written through the symlink into the victim file")
        self.assertFalse((out / "001.json").is_symlink(),
                         "the renamed response is a symlink pointing outside the outbox")

    def test_respond_refuses_an_outbox_that_is_a_symlink_out_of_agents_root(self):
        # A caller directory whose outbox/ is a symlink redirects every reply
        # the agent writes to an arbitrary directory. The destination must be a
        # real directory under AGENTS_ROOT.
        elsewhere = self.tmp / "elsewhere"
        elsewhere.mkdir()
        client = self.agents_root / "client"
        client.mkdir(parents=True)
        (client / "outbox").symlink_to(elsewhere, target_is_directory=True)
        self.queue(eid="001", caller="client")
        self.agent.tick()
        self.assertFalse((elsewhere / "001.json").exists(),
                         "reply landed outside AGENTS_ROOT via a symlinked outbox")

    def test_symlinked_request_in_the_inbox_is_not_followed(self):
        # The bytes of a request must be the bytes placed in the inbox. A symlink
        # named like a request reads (and later unlinks) something else; with a
        # target such as /dev/zero the read never ends. It must be quarantined
        # or skipped, never handled.
        outside = self.tmp / "outside.json"
        outside.write_text(json.dumps({"event_id": "000", "kind": "work", "caller": "client"}))
        (self.inbox() / "000.json").symlink_to(outside)
        self.agent.tick()
        self.assertEqual(self.agent.calls, [], "a symlinked inbox entry reached handle()")
        self.assertFalse((comms.outbox_dir("client") / "000.json").exists())

    def test_fifo_named_like_a_request_does_not_hang_the_tick(self):
        # A FIFO in the inbox blocks read_bytes() until a writer appears; the
        # daemon stops heartbeating forever. Only regular files may be read.
        fifo = self.inbox() / "000.json"
        os.mkfifo(fifo)
        self.queue(eid="001")
        done = threading.Event()

        def run():
            try:
                self.agent.tick()
            finally:
                done.set()

        t = threading.Thread(target=run, daemon=True)

        def unblock():
            # Release a blocked reader so the thread finishes before the
            # patched roots are restored; harmless if nothing is blocked.
            try:
                fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
                os.close(fd)
            except OSError:
                pass
            t.join(5)
        self.addCleanup(unblock)
        t.start()
        self.assertTrue(done.wait(3), "tick() blocked on a FIFO in the inbox")
        self.assertTrue((comms.outbox_dir("client") / "001.json").exists(),
                        "the request behind the FIFO was never processed")


class InputBoundary(TrustTestCase):
    def test_oversized_request_is_rejected_by_size_before_it_is_read(self):
        # MAX_MESSAGE_BYTES is enforced after read_bytes() has already loaded
        # the whole file. A multi-GiB request is read into memory first; when
        # that allocation fails the error is neither OSError nor ValueError, so
        # it escapes drain_inbox and every later message in the inbox is
        # skipped, tick after tick. Here the allocation failure is simulated.
        huge = self.inbox() / "000.json"
        with huge.open("wb") as fh:
            fh.truncate(2 * 1024 ** 3)   # sparse, costs no disk
        self.queue(eid="001")
        real_read_bytes = Path.read_bytes

        def guarded_read_bytes(p):
            if p.stat().st_size > 64 * 1024 * 1024:
                raise MemoryError("simulated: cannot allocate the whole request")
            return real_read_bytes(p)

        with patch.object(Path, "read_bytes", guarded_read_bytes):
            self.agent.tick()
        self.assertTrue((comms.outbox_dir("client") / "001.json").exists(),
                        "a huge file ahead of it kept a valid request from being processed")
        self.assertIn("000.json", self.quarantined_requests())
        reason = json.loads((self.quarantine_dir() / "000.json.reason.json").read_text())
        self.assertEqual(reason["reason_code"], "oversized")

    def test_quarantine_sidecar_cannot_overwrite_a_quarantined_request(self):
        # A request file named `<x>.json.reason.json` is a valid inbox name. Once
        # quarantined it occupies the sidecar name of a later quarantined
        # `<x>.json`, whose sidecar write then replaces the preserved bytes.
        (self.inbox() / "x.json.reason.json").write_text("attacker-bytes")
        list(comms.drain_inbox("worker"))
        (self.inbox() / "x.json").write_text("{bad")
        list(comms.drain_inbox("worker"))
        contents = [p.read_text() for p in self.quarantine_dir().iterdir()]
        self.assertIn("attacker-bytes", contents,
                      "quarantined bytes were overwritten by a sidecar of a later quarantine")

    def test_quarantine_collision_within_one_second_keeps_every_file(self):
        # The collision suffix is a second-resolution timestamp; two collisions
        # in the same second rename over each other and the earlier bytes are
        # lost. Bytes in quarantine must never be replaced.
        with patch.object(comms, "now_iso", lambda: "2026-01-01T00:00:00Z"):
            for body in ("first", "second", "third"):
                (self.inbox() / "000.json").write_text(body)
                list(comms.drain_inbox("worker"))
        contents = sorted(p.read_text() for p in self.quarantine_dir().glob("000.json*")
                          if not p.name.endswith(".reason.json"))
        self.assertEqual(contents, ["first", "second", "third"])


class CallerIdentity(TrustTestCase):
    def test_reserved_root_names_cannot_be_used_as_a_caller(self):
        # `_registry.json` is a valid identifier. Replying to it creates the
        # directory AGENTS_ROOT/_registry.json/outbox, after which
        # known_agents() finds a directory where it expects a file and every
        # send_directive() in the fabric raises. One unauthenticated message.
        self.queue(eid="001", caller="_registry.json")
        self.agent.tick()
        self.assertFalse((self.agents_root / "_registry.json").exists(),
                         "a caller name created AGENTS_ROOT/_registry.json")
        self.assertIsNone(comms.known_agents())
        self.assertIsNone(comms.send_directive("worker", {"kind": "work", "event_id": "d1"},
                                               caller="client", timeout_s=0))
        self.assertEqual([p for p in self.agents_root.iterdir() if p.name.startswith("_")
                          and p.name not in ("_audit_fallback", "_heartbeats_local", "_trust")], [])

    def test_unknown_caller_is_refused_when_a_registry_exists(self):
        # send_directive honours _registry.json for targets, but the receiving
        # side accepts any identifier as a caller and materialises a new agent
        # directory for it. With a registry present, an unlisted caller must
        # not gain a directory under AGENTS_ROOT nor a handled request.
        (self.agents_root / "_registry.json").write_text(json.dumps(["worker", "client"]))
        self.queue(eid="001", caller="stranger")
        self.agent.tick()
        self.assertFalse((self.agents_root / "stranger").exists(),
                         "an unregistered caller obtained a directory under AGENTS_ROOT")
        self.assertEqual(self.agent.calls, [], "an unregistered caller's request reached handle()")

    def test_heartbeat_and_bus_health_do_not_accept_path_like_agent_names(self):
        # heartbeat() and write_bus_health() build filenames from the agent name
        # without validate_identifier; "../escape" writes beside the root.
        # CONTRACT guarantee 1: identifiers are not paths.
        comms.heartbeat("../escape")
        try:
            comms.write_bus_health("../escape", {"x": 1})
        except ValueError:
            pass
        self.assertFalse((self.tmp / "escape.heartbeat").exists(), "heartbeat escaped LOCAL_HB_DIR / HB_DIR")
        self.assertFalse((self.tmp / "escape_summary.json").exists(), "bus health escaped BH_DIR")


class PrivilegedKinds(TrustTestCase):
    def install_key(self):
        key = b"operator-secret"
        trust = self.agents_root / "_trust"
        trust.mkdir(parents=True, exist_ok=True)
        (trust / "operator.key").write_bytes(key)
        return key

    def test_signed_invoke_skill_arguments_cannot_be_altered_in_transit(self):
        # The HMAC covers event_id|kind|action|caller only. Anyone who can
        # rewrite a pending request file keeps the signature valid while
        # changing args/kwargs: an authorised "deploy staging" becomes
        # "deploy production" with the operator's own signature.
        key = self.install_key()
        invoker = types.SimpleNamespace(invoke=Mock(return_value={"ok": True}), discover=Mock(return_value=[]))
        with patch.object(agent_base, "skill_invoker", invoker):
            auth = comms.sign_directive(key, "001", "invoke_skill", "deploy", "client")
            # what the operator signed: args ["staging"]; what is actually queued:
            self.queue(eid="001", caller="client", kind="invoke_skill", skill="deploy",
                       args=["production"], kwargs={"force": True}, auth=auth)
            self.agent.tick()
        for call in invoker.invoke.call_args_list:
            self.assertNotIn("production", call.args[1],
                             "tampered arguments ran under an unchanged signature")
        reply = json.loads((comms.outbox_dir("client") / "001.json").read_text())
        self.assertIs(reply["ok"], False)
        self.assertEqual(reply.get("failure_class"), "rejection")

    def test_invoke_skill_kwargs_that_collide_with_transport_parameters_are_rejected(self):
        # kwargs pass the "dict with string keys" check but `name` collides with
        # invoke_skill's first parameter and `agent` with the identity the agent
        # injects; both raise and are counted as infrastructure failures, so five
        # such requests from anyone trip the breaker. They are bad requests.
        invoker = types.SimpleNamespace(invoke=Mock(return_value={"ok": True}), discover=Mock(return_value=[]))
        with patch.object(agent_base, "skill_invoker", invoker):
            self.queue(eid="001", kind="invoke_skill", skill="s", args=[], kwargs={"name": "x"})
            self.queue(eid="002", kind="invoke_skill", skill="s", args=[], kwargs={"agent": "spoofed"})
            self.agent.tick()
        for eid in ("001", "002"):
            reply = json.loads((comms.outbox_dir("client") / f"{eid}.json").read_text())
            self.assertIs(reply["ok"], False, eid)
            self.assertEqual(reply.get("failure_class"), "rejection", eid)
        self.assertEqual(self.agent._consec_failures, 0)
        for call in invoker.invoke.call_args_list:
            self.assertEqual(call.kwargs.get("agent"), "worker", "caller-supplied kwargs overrode agent identity")


# ---------------------------------------------------------------------------
# Round 2, same lens. Each test below documents the break it is looking for
# in its own docstring; the assertion message names the observed behaviour.
# ---------------------------------------------------------------------------

class Other(BaseAgent):
    """A second agent beside `worker`, for cross-agent tests."""
    name = "other"
    cadence_s = 0

    def __init__(self):
        self.calls = []
        super().__init__()

    def handle(self, req):
        self.calls.append(req)
        return {"ok": True}

    def idle_cycle(self):
        pass


class PrivilegedKindsRound2(TrustTestCase):
    def install_key(self, key=b"operator-secret"):
        trust = self.agents_root / "_trust"
        trust.mkdir(parents=True, exist_ok=True)
        (trust / "operator.key").write_bytes(key)
        return key

    def replies(self, caller="client"):
        out = comms.outbox_dir(caller)
        return {p.stem: json.loads(p.read_text()) for p in out.glob("*.json")}

    def test_signed_privileged_request_cannot_be_replayed_to_a_different_agent(self):
        """Guarantee 22 says a signed privileged directive "cannot be re-targeted
        by rewriting the pending file". The HMAC covers event_id, kind, action,
        caller, args and kwargs but NOT the agent the operator addressed. The
        same signed bytes, copied unchanged into any other agent's inbox under
        the same filename, verify there too: one `heal: respawn` signed for one
        daemon restarts every daemon in the fabric; one signed `invoke_skill`
        runs on every agent. The receiving agent must reject a signature that
        was not produced for it."""
        key = self.install_key()
        other = Other()
        self.addCleanup(self.release, other)
        # The operator signs a heal for `worker` only.
        auth = comms.sign_directive(key, "001", "heal", "rewrite_north_star", "client")
        signed = {"event_id": "001", "kind": "heal", "action": "rewrite_north_star",
                  "caller": "client", "auth": auth}
        (comms.inbox_dir("other") / "001.json").write_text(json.dumps(signed))
        other.tick()
        self.assertFalse((other.self_dir / "NORTH_STAR.md").exists(),
                         "a heal signed for `worker` executed on `other` (signature does not bind the target agent)")
        reply = self.replies()["001"]
        self.assertIs(reply["ok"], False)
        self.assertEqual(reply.get("failure_class"), "rejection")

    def test_non_ascii_auth_is_a_rejection_not_a_counted_failure(self):
        """Guarantee 10/22: a missing or wrong signature is a `rejection`, which
        never counts toward the breaker. hmac.compare_digest raises TypeError
        for a str containing non-ASCII characters; that TypeError is caught by
        the handler boundary as "handler raised" and classed `failure`. Five
        unauthenticated heal requests carrying `auth: "é"` therefore trip
        the breaker and pause the agent's ordinary work for the cooldown, and
        `heal` stays reachable while open so the sender can keep it there."""
        self.install_key()
        for i in range(5):
            self.queue(eid=f"00{i}", kind="heal", action="rewrite_north_star", auth="é")
        self.agent.tick()
        self.assertEqual(self.agent._breaker_state, "closed",
                         "five wrong signatures from an unverified caller tripped the breaker")
        self.assertEqual(self.agent._consec_failures, 0,
                         "a wrong signature was counted as an infrastructure failure")
        for eid, reply in self.replies().items():
            self.assertIs(reply["ok"], False, eid)
            self.assertEqual(reply.get("failure_class"), "rejection", eid)

    def test_empty_operator_key_is_not_reported_as_enforced_auth(self):
        """health.privileged_auth_enforced is `key_path.exists()`. An empty key
        file exists, so health says enforcement is on, while anyone can sign
        with b"" and every privileged directive verifies. Either the agent
        refuses to run with an empty key or health must not claim enforcement."""
        self.install_key(b"")
        auth = comms.sign_directive(b"", "001", "heal", "rewrite_north_star", "client")
        self.queue(eid="001", kind="heal", action="rewrite_north_star", auth=auth)
        self.agent.tick()
        health = json.loads((self.agent.self_dir / "state" / "health.json").read_text())
        executed = (self.agent.self_dir / "NORTH_STAR.md").exists()
        self.assertFalse(health["privileged_auth_enforced"] and executed,
                         "health reports privileged auth enforced while a b'' signature was accepted")


class RequestRejectionClass(TrustTestCase):
    def test_plan_with_unencodable_text_is_a_rejection_not_a_counted_failure(self):
        """`plan` is unprivileged and its arguments are "checked before use"
        (guarantee 8). JSON allows a lone surrogate escape (`"\\udc80"`), which
        json.loads turns into a str that UTF-8 cannot encode. It passes the
        isinstance checks, plan() raises UnicodeEncodeError writing PLAN.md, the
        boundary classes it `failure`, and five such requests from anyone trip
        the breaker. Text the request supplied that cannot be written is a bad
        request, not an unhealthy agent."""
        for i in range(5):
            self.queue(eid=f"00{i}", kind="plan", title="t", steps=["\udc80"], rationale="r")
        self.agent.tick()
        self.assertEqual(self.agent._consec_failures, 0,
                         "five plan requests carrying unencodable text were counted as infrastructure failures")
        self.assertEqual(self.agent._breaker_state, "closed")


class InputBoundaryRound2(TrustTestCase):
    def test_deeply_nested_request_does_not_block_later_messages(self):
        """Guarantee 3: undecodable input is quarantined `decode_error` and
        never stops the batch. read_message only catches ValueError around
        json.loads; a JSON document nested ~500k deep fits under the 1 MiB
        cap and raises RecursionError instead. It escapes drain_inbox, tick
        audits `inbox_exception`, and because the inbox is drained in name
        order every request sorting after it is skipped, tick after tick,
        until an operator removes the file by hand."""
        depth = 500_000
        body = '{"kind":"work","caller":"client","x":' + "[" * depth + "]" * depth + "}"
        self.assertLessEqual(len(body), comms.MAX_MESSAGE_BYTES)
        (self.inbox() / "000.json").write_text(body)
        self.queue(eid="001")
        self.agent.tick()
        self.assertTrue((comms.outbox_dir("client") / "001.json").exists(),
                        "a deeply nested file ahead of it kept a valid request from being processed")
        self.assertFalse((self.inbox() / "000.json").exists(),
                         "the undecodable file was neither quarantined nor otherwise moved out of the way")
        self.assertEqual(self.events("inbox_exception"), [])

    def test_bad_request_with_a_maximal_filename_is_quarantined_not_retried_forever(self):
        """Guarantee 3: bad input is moved into quarantine/ with a sidecar. The
        sidecar name is `<original name>.reason.json`; an inbox entry whose
        name is already NAME_MAX (255) long makes that open() fail with
        ENAMETOOLONG, quarantine_message audits `inbox_quarantine_failed`,
        returns None, and the file stays in the inbox. It is re-read, re-fails
        and re-audited every tick, inflates inbox_pending, and 21 of them keep
        DREAM_INBOX_BACKLOG firing. Bad input must leave the inbox once."""
        name = "a" * 250 + ".json"           # stem > 128 chars: bad_identifier
        (self.inbox() / name).write_text("{bad")
        self.agent.tick()
        self.agent.tick()
        self.assertNotIn(name, [p.name for p in self.inbox().iterdir()],
                         "bad input stayed in the inbox and is retried every tick")
        contents = [p.read_text() for p in self.quarantine_dir().iterdir()
                    if not p.name.endswith(".reason.json")]
        self.assertIn("{bad", contents, "the bytes were not preserved in quarantine/")


class SenderSideSymlinks(TrustTestCase):
    def test_send_directive_does_not_write_through_a_planted_symlink_in_the_target_inbox(self):
        """Every sender writes into a target's inbox, so any sender can plant a
        `<eid>.json.tmp` symlink there ahead of another sender's request with
        a predictable event id. send_directive uses Path.write_text on the
        temp name, which follows the link: the victim file (anything the
        sending uid may write) is truncated and overwritten with the request
        body, and the following rename turns the request itself into a symlink
        the receiving agent quarantines. The same exclusive create respond()
        uses (guarantee 2) must apply to the sender's temp file."""
        victim = self.tmp / "victim.txt"
        victim.write_text("precious")
        (self.inbox() / "d1.json.tmp").symlink_to(victim)
        comms.send_directive("worker", {"kind": "work", "event_id": "d1"}, caller="client", timeout_s=0)
        self.assertEqual(victim.read_text(), "precious",
                         "send_directive wrote the request through a symlink planted in the target inbox")
        self.assertFalse((self.inbox() / "d1.json").is_symlink(),
                         "the delivered request is a symlink pointing outside the inbox")


if __name__ == "__main__":
    unittest.main()

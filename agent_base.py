# ============================================================================
# PUBLIC COPY — the loop every agent in the fabric ran
# ============================================================================
# Verbatim from `_shared/agent_base.py` (private repo StanHus/aptraining-mesh).
# Only this comment block is new. Local imports: `comms.py` (included), `bd`
# (the private bead ledger; a labelled no-op stub `bd.py` is included so this
# file imports standalone) and `skill_invoker` (private, imported defensively).
#
# READ tick() FIRST. It is the whole contract: drain the inbox, handle four
# universal directive kinds (heal, dream, plan, invoke_skill) or hand off to the
# subclass, reply, keep a circuit breaker, audit every event, run the agent's own
# idle work, dream twice a day, heartbeat. 77 lines. 56 daemons overrode only
# handle() and idle_cycle(). A tripped breaker keeps heartbeating so the mesh
# doctor can tell "broken" from "dead".
#
# PRIVATE FILES REFERENCED BELOW:
#   OPERATING_STANDARDS.md  nine standing rules; every agent records the version
#                           it loaded, so compliance is provable, not assumed.
#   bd.py                   bead ledger; only BEAD_WORTHY_KINDS create entries
#                           (an earlier version logged every directive and buried
#                           the real work).
#   skill_invoker.py        lets any agent drive any other agent's skill over the bus.
#   <agent>/NORTH_STAR.md, plans/, learnings/   per-agent state the dream sequence
#                           audits and the plan() method writes before acting.
# ============================================================================
"""_shared.agent_base — base class providing planning + dream sequence + healing receptivity.

Every macmini agent should subclass `BaseAgent` and override `tick()`.

Standing rules baked in:
- Plan before non-trivial actions (PLAN.md written to state/ before action)
- Dream sequence twice daily (self-doctor: audit own state, refresh NORTH_STAR notes,
  flag anomalies)
- Healing receptivity: accepts `kind:heal` and `kind:plan` directives from peers
- Self-documentation: every decision appended to learnings/<DATE>.md
- bd integration: every directive received → bd_create; on close → bd_close
- Operating standards awareness: every agent loads _shared/OPERATING_STANDARDS.md on
  init and records `standards loaded vN` in its audit + bus_health (Stan standing rules).
"""
import json, os, time, subprocess
from pathlib import Path
from datetime import datetime, timezone, time as dtime

from comms import (
    AGENTS_ROOT, now_iso, heartbeat, audit, write_bus_health, alert_main,
    inbox_dir, outbox_dir, drain_inbox, respond, request_token, new_event_id,
)

# Canonical fabric-wide standing rules. Loaded on every agent init so every
# daemon is provably aware of Stan's standing directives (continuity mandate,
# relative-only external channels, strong-Synapse bar, real-eval recipe, chain
# discipline, resolve-items-only, no other-team mutation). See the file's header
# for the version + date. _load_operating_standards() parses the Version line.
OPERATING_STANDARDS_PATH = (
    Path(os.path.expanduser("~")) / "aptraining_agents/_shared/OPERATING_STANDARDS.md"
)
from bd import bd_create, bd_close, bd_note, bd_status

# Wave 1.5 — guarded skill-reach import. A bad/missing registry or module must
# NEVER crash a daemon, so this is fully defensive: on ANY failure skill_invoker
# is None and every skill method degrades to a structured error.
try:
    import skill_invoker  # type: ignore
except Exception:
    skill_invoker = None

class BaseAgent:
    """Subclass and override `handle(req: dict) -> dict` + optionally `idle_cycle()`."""
    name: str = "unnamed"
    cadence_s: int = 60
    dream_times_utc = (dtime(6, 30), dtime(18, 30))  # twice daily
    # Bead-noise fix 2026-06-03 (bead aptraining-7gecb): routine directive handling
    # (dreams, idle echoes, run_eval/ib.evaluate/etc.) must NOT spawn persistent task
    # beads. The audit-log (directive_handled / traced_*) is the retrace trail. ONLY
    # kinds in this allowlist get a real create+close task bead -- for genuine units
    # of work. Subclasses opt-in specific kinds. Default: none (no directive beads).
    BEAD_WORTHY_KINDS: tuple = ()

    def __init__(self):
        self.self_dir = AGENTS_ROOT / self.name
        for sub in ("inbox", "outbox", "state", "learnings", "logs", "delegations", "plans"):
            (self.self_dir / sub).mkdir(parents=True, exist_ok=True)
        self._last_dream_date = None
        self._my_skills = None  # Wave 1.5 — lazy-loaded skills.json cache
        # Circuit breaker: pause agent on N consecutive handler failures
        self._consec_failures = 0
        self._circuit_breaker_threshold = 5
        self._circuit_breaker_pause_until = 0
        # bd-stall guard: corrupt dolt ledger can hang the bd subprocess.
        # One failure flips this and we skip bd for the process lifetime so a
        # hanging/corrupt bd NEVER blocks tick() or the heartbeat write.
        self._bd_degraded = False
        # Operating-standards awareness — never let a missing/garbled file crash init.
        self._operating_standards_version = None
        try:
            self._load_operating_standards()
        except Exception as e:
            audit(self.name, "operating_standards_load_error", error=str(e)[:160])
        audit(self.name, "startup", pid=os.getpid(), cadence_s=self.cadence_s,
              operating_standards=self._operating_standards_version)

    def _load_operating_standards(self):
        """Load _shared/OPERATING_STANDARDS.md into the agent's awareness.

        Records `standards loaded vN` in the audit trail and stamps the version
        into bus_health so the whole fabric is provably aware of Stan's standing
        rules. Fully defensive: a missing/garbled file degrades to version
        'absent' and NEVER blocks init or the heartbeat.
        """
        p = OPERATING_STANDARDS_PATH
        version = "absent"
        text = ""
        if p.exists():
            try:
                text = p.read_text()
            except Exception:
                text = ""
            for line in text.splitlines():
                s = line.strip()
                # matches "**Version:** v1" or "Version: v1"
                if s.lower().lstrip("*").strip().startswith("version"):
                    version = s.split(":", 1)[-1].replace("*", "").strip() or "unknown"
                    break
            else:
                version = "unknown"
        self._operating_standards_version = version
        # Keep the full text addressable for prompt injection / self-reference.
        self.operating_standards_text = text
        audit(self.name, "operating_standards_loaded", version=version,
              present=p.exists(), bytes=len(text))
        # Stamp into bus_health so mesh-doctor / monitors can confirm awareness.
        try:
            write_bus_health(self.name, {
                "operating_standards_version": version,
                "operating_standards_present": p.exists(),
                "note": f"standards loaded {version}",
            })
        except Exception:
            pass
        return version

    def operating_standards(self) -> str:
        """Return the loaded OPERATING_STANDARDS.md text ('' if absent). For
        injecting Stan's standing rules into an agent's LLM prompts."""
        return getattr(self, "operating_standards_text", "") or ""

    def plan(self, title, requirements=None, steps=None, decision_gates=None,
             failure_scenarios=None, insights=None, dependencies=None, rationale=""):
        """Write a PLAN.md following the Trilogy 'perfect plan' methodology.

        6 sections:
          1. Requirements Audit — every constraint logged verbatim
          2. Dependency Graph — bd-style beads with priority + blocked-by
          3. Steps — ordered action list
          4. Decision Gates — pass / adjust / abort criteria per phase
          5. Failure Scenarios — F1, F2 … with detection + cascading recovery
          6. Insight Prioritization — T1 (blocking) / T2 (significant) / T3 (incremental)

        Args:
          title: short title
          requirements: list of strings (verbatim constraints from caller)
          steps: list of strings (ordered actions)
          decision_gates: list of dicts {phase, condition, pass, adjust, abort}
          failure_scenarios: list of dicts {id, name, detection, recovery}
          insights: list of dicts {tier, finding}
          dependencies: list of strings (e.g., "blocked_by: aptraining-h7o")
          rationale: free text explaining why this plan
        """
        requirements = requirements or []
        steps = steps or []
        decision_gates = decision_gates or []
        failure_scenarios = failure_scenarios or []
        insights = insights or []
        dependencies = dependencies or []

        ts = now_iso().replace(":", "")
        p = self.self_dir / "plans" / f"{ts}_{title.replace(' ','_')[:60]}.md"
        body = [f"# {self.name} PLAN — {title}", "",
                f"**Created**: {now_iso()}",
                "**Methodology**: Trilogy Perfect Plan (6-section)", ""]
        if rationale:
            body += ["## Rationale", "", rationale, ""]

        body += ["## 1. Requirements Audit", ""]
        if requirements:
            for r in requirements: body.append(f"- {r}")
        else:
            body.append("_(none — inferred from caller context)_")
        body.append("")

        body += ["## 2. Dependency Graph (beads)", ""]
        if dependencies:
            for d in dependencies: body.append(f"- {d}")
        else:
            body.append("_(no upstream blockers)_")
        body.append("")

        body += ["## 3. Steps", ""]
        for i, step in enumerate(steps, 1):
            body.append(f"{i}. {step}")
        body.append("")

        body += ["## 4. Decision Gates", ""]
        if decision_gates:
            for g in decision_gates:
                body.append(f"### Phase: {g.get('phase','?')}")
                body.append(f"- **Condition**: {g.get('condition','?')}")
                body.append(f"- **Pass**: {g.get('pass','?')}")
                body.append(f"- **Adjust**: {g.get('adjust','?')}")
                body.append(f"- **Abort**: {g.get('abort','?')}")
                body.append("")
        else:
            body += ["_(no explicit gates; default: stop and re-plan if any step fails twice)_", ""]

        body += ["## 5. Failure Scenarios", ""]
        if failure_scenarios:
            for f in failure_scenarios:
                body.append(f"- **{f.get('id','F?')} {f.get('name','unnamed')}** — Detection: {f.get('detection','?')} → Recovery: {f.get('recovery','?')}")
        else:
            body.append("_(no scenarios catalogued — agent will audit & alert main-a on any anomaly)_")
        body.append("")

        body += ["## 6. Insight Prioritization", ""]
        if insights:
            for ins in insights:
                body.append(f"- **T{ins.get('tier','?')}**: {ins.get('finding','?')}")
        else:
            body.append("_(no prior insights surfaced)_")
        body.append("")

        body += ["## Standing rules (apply throughout)", "",
                 "- Stop and re-plan if any step fails twice",
                 "- bd_create on directive enter, bd_close on completion (the retrace trail)",
                 "- Audit every action via comms.audit()",
                 "- Never violate the inviolable: build-upon chain / no other-team mutation / Rule 0 (no direct FW POST) / Stan-never-blocker / proactive-mesh-comms",
                 ""]
        p.write_text("\n".join(body))
        audit(self.name, "plan_written", path=p.name, steps=len(steps), gates=len(decision_gates), failures=len(failure_scenarios))
        return p

    def learn(self, kind: str, body: str):
        """Append a learning to learnings/<DATE>.md."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        f = self.self_dir / "learnings" / f"{today}.md"
        with f.open("a") as out:
            out.write(f"\n## {now_iso()} — {kind}\n\n{body}\n")
        audit(self.name, "learning_logged", kind=kind)

    def dream_sequence(self):
        """Self-doctor: audit own state + check NORTH_STAR + flag anomalies + emit summary."""
        ts = now_iso()
        ns = self.self_dir / "NORTH_STAR.md"
        ns_present = ns.exists()
        recent_audit_lines = self._count_audit_today()
        inbox_pending = len(list(inbox_dir(self.name).glob("*.json")))
        outbox_pending = len(list(outbox_dir(self.name).glob("*.json")))
        # Re-affirm operating-standards awareness on every dream (twice daily).
        try:
            self._load_operating_standards()
        except Exception:
            pass
        summary = {
            "north_star_present": ns_present,
            "audit_lines_today": recent_audit_lines,
            "inbox_pending": inbox_pending,
            "outbox_pending": outbox_pending,
            "operating_standards_version": self._operating_standards_version,
        }
        self.learn("dream_sequence", json.dumps(summary, indent=2))
        audit(self.name, "dream_sequence", **summary)
        # Flag if anomalies
        if not ns_present:
            alert_main(self.name, "DREAM_NORTH_STAR_MISSING", f"# {self.name} dream: NORTH_STAR.md missing\n\n{ts}\n\nNorth Star not found in agent home. Re-creation required.\n")
        if inbox_pending > 20:
            alert_main(self.name, "DREAM_INBOX_BACKLOG", f"# {self.name} dream: inbox backlog\n\n{ts}\n\nPending: {inbox_pending} directives. Investigate stuck queue.\n")

    def _count_audit_today(self) -> int:
        from comms import AUDIT_DIR
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        f = AUDIT_DIR / f"{today}_{self.name}.jsonl"
        if not f.exists(): return 0
        try: return sum(1 for _ in f.open())
        except Exception: return 0

    def _maybe_dream(self):
        """Run dream sequence at scheduled UTC times once per day."""
        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()
        for target in self.dream_times_utc:
            window_start = datetime.combine(today, target).replace(tzinfo=timezone.utc)
            window_end = window_start.replace(minute=window_start.minute + 5)
            if window_start <= now_utc <= window_end and self._last_dream_date != (today, target):
                self.dream_sequence()
                self._last_dream_date = (today, target)
                break

    def healing_handler(self, req: dict) -> dict:
        """Built-in handler for kind:heal — accept restart / state-reset / re-init directives."""
        action = req.get("action", "soft_reset")
        if action == "soft_reset":
            for k in ("inbox", "outbox"):
                d = self.self_dir / k
                for f in d.glob("*.tmp"):
                    try: f.unlink()
                    except Exception: pass
            audit(self.name, "self_heal", action=action)
            return {"ok": True, "healed": action}
        if action == "respawn":
            audit(self.name, "self_heal_respawn_requested")
            # We exit; launchd KeepAlive=true will restart us
            os._exit(0)
        if action == "rewrite_north_star":
            ns = self.self_dir / "NORTH_STAR.md"
            if not ns.exists():
                ns.write_text(f"# {self.name} — NORTH_STAR\n\nRegenerated {now_iso()} by self-heal directive.\n")
            return {"ok": True, "healed": "north_star_regen"}
        return {"ok": False, "error": f"unknown heal action {action!r}"}

    def validate_directive(self, req, required=None, kinds=None):
        """Returns (ok, error_msg). Use in handler() before any work."""
        required = required or []
        for k in required:
            if k not in req or req.get(k) in (None, ""):
                return False, f"missing required field: {k}"
        if kinds is not None and req.get("kind") not in kinds:
            return False, f"unsupported kind {req.get(chr(39)+chr(107)+chr(105)+chr(110)+chr(100)+chr(39))!r}"
        return True, None

    def _circuit_breaker_open(self):
        return time.time() < self._circuit_breaker_pause_until

    def _trip_breaker(self, reason):
        from comms import alert_main
        self._circuit_breaker_pause_until = time.time() + 300
        alert_main(self.name, "CIRCUIT_BREAKER_TRIPPED",
                   f"# {self.name} - circuit breaker TRIPPED\\n\\n**reason**: {reason}\\n**consecutive failures**: {self._consec_failures}\\n")
        audit(self.name, "circuit_breaker_tripped", reason=reason, consec=self._consec_failures)

    # ---- Wave 1.5 skill-reach surface (all guarded — never raise) ----------
    def invoke_skill(self, name, *args, **kw):
        """Invoke a registry skill by name. Returns skill_invoker's structured
        dict; on missing module returns a structured error (never raises).
        Audit is performed inside skill_invoker.invoke()."""
        if skill_invoker is None:
            return {"ok": False, "skill": name, "kind": "unknown",
                    "error": "skill_invoker unavailable (guarded import failed)"}
        try:
            return skill_invoker.invoke(name, list(args), agent=self.name, **kw)
        except Exception as e:
            return {"ok": False, "skill": name, "kind": "unknown",
                    "error": f"invoke raised: {str(e)[:200]}"}

    def discover_skills(self, keyword, limit=20):
        """Search the skill registry. Returns [] on missing module / error."""
        if skill_invoker is None:
            return []
        try:
            return skill_invoker.discover(keyword, limit)
        except Exception:
            return []

    def my_skills(self):
        """Lazy-load + cache this agent's skills.json. Returns {} on any error."""
        if self._my_skills is not None:
            return self._my_skills
        try:
            p = self.self_dir / "skills.json"
            self._my_skills = json.loads(p.read_text())
        except Exception:
            self._my_skills = {}
        return self._my_skills

    def skill_catalog_text(self):
        """Compact `name — description` block of this agent's domain + base
        skills, for injecting into LLM prompts. Returns '' on any error."""
        try:
            sk = self.my_skills() or {}
            lines = []
            for group in ("domain_skills", "base_skills"):
                for s in (sk.get(group) or []):
                    if isinstance(s, dict):
                        nm = s.get("name") or s.get("handler") or "?"
                        desc = (s.get("description") or s.get("status") or "").strip()
                        lines.append(f"- {nm} — {desc}"[:160] if desc else f"- {nm}")
                    elif isinstance(s, str):
                        lines.append(f"- {s}")
            return "\n".join(lines)
        except Exception:
            return ""

    def handle(self, req: dict) -> dict:
        """Override in subclass. Default returns unknown-kind error."""
        return {"ok": False, "error": f"agent {self.name} has no handler for kind={req.get('kind')!r}"}

    def idle_cycle(self):
        """Override for poll-style agents that do periodic work without inbox directives."""
        pass

    def _bd_trace_create(self, **kw):
        """Time-bounded, non-fatal bd_create. A corrupt/hanging bd must never
        block the tick. bd.py already enforces a subprocess timeout; here we
        treat any failure (None / exception) as terminal: flip _bd_degraded so
        we stop paying the timeout, audit once, and continue without a bead."""
        if self._bd_degraded:
            return None
        try:
            bid = bd_create(**kw)
        except Exception as e:
            bid = None
            self._bd_degraded = True
            audit(self.name, "bd_degraded", reason="bd_create raised", error=str(e)[:160])
            return None
        if bid is None:
            self._bd_degraded = True
            audit(self.name, "bd_degraded", reason="bd_create returned None (timeout/corrupt ledger); skipping bd-trace for process lifetime")
        return bid

    def _bd_trace_close(self, bead_id, **kw):
        """Time-bounded, non-fatal bd_close. Never blocks tick/heartbeat."""
        if not bead_id or self._bd_degraded:
            return False
        try:
            ok = bd_close(bead_id, **kw)
        except Exception as e:
            self._bd_degraded = True
            audit(self.name, "bd_degraded", reason="bd_close raised", error=str(e)[:160])
            return False
        if not ok:
            self._bd_degraded = True
            audit(self.name, "bd_degraded", reason="bd_close failed (timeout/corrupt ledger); skipping bd-trace for process lifetime")
        return ok

    def tick(self):
        if self._circuit_breaker_open():
            heartbeat(self.name)
            return  # still heartbeat so mesh-doctor knows we're alive
        # 1. Process inbox
        for f, req in drain_inbox(self.name):
            eid = req.get("event_id", f.stem)
            kind = req.get("kind", "")
            caller = req.get("caller", "main-a")
            # Bead trace -- ONLY for genuinely bead-worthy work (opt-in allowlist).
            # Routine directives (dream, run_eval echoes, idle ops) are audit-logged
            # via directive_handled below and must NOT pollute bd list. See
            # BEAD_WORTHY_KINDS + bead-noise fix 2026-06-03 (aptraining-7gecb).
            bead_id = None
            if kind in self.BEAD_WORTHY_KINDS:
                bead_id = self._bd_trace_create(
                    title=f"[{self.name}] {kind} from {caller}",
                    priority=2,
                    description=f"event_id={eid}\nkind={kind}\ncaller={caller}\nreceived={now_iso()}",
                    labels=[f"agent:{self.name}", f"kind:{kind}"],
                )
            if kind == "heal":
                result = self.healing_handler(req)
            elif kind == "dream":
                self.dream_sequence()
                result = {"ok": True, "dream": "completed"}
            elif kind == "plan":
                title = req.get("title", "directed-plan")
                steps = req.get("steps", [])
                rationale = req.get("rationale", "")
                p = self.plan(title, steps, rationale)
                result = {"ok": True, "plan_path": str(p)}
            elif kind == "invoke_skill":
                # Wave 1.5 — any agent/main-a can drive any skill over the bus.
                result = self.invoke_skill(
                    req.get("skill"),
                    *req.get("args", []),
                    **req.get("kwargs", {}),
                )
            else:
                try:
                    result = self.handle(req)
                except Exception as e:
                    audit(self.name, "handler_exception", eid=eid, kind=kind, error=str(e)[:200])
                    result = {"ok": False, "error": f"handler raised: {str(e)[:200]}"}
            respond(caller, eid, result)
            # Circuit breaker bookkeeping
            if result.get("ok"):
                self._consec_failures = 0
            else:
                self._consec_failures += 1
                if self._consec_failures >= self._circuit_breaker_threshold:
                    self._trip_breaker(f"5+ consecutive handler failures (last: {result.get('error','?')[:100]})")
            if bead_id:
                if result.get("ok"):
                    self._bd_trace_close(bead_id, reason="completed", note=json.dumps({"caller": caller, "kind": kind, "result_ok": True})[:200])
                else:
                    self._bd_trace_close(bead_id, reason="failed", note=str(result.get("error","unknown"))[:200])
            try: f.unlink()
            except Exception: pass
            audit(self.name, "directive_handled", eid=eid, kind=kind, caller=caller, ok=result.get("ok", False))

        # 2. Idle cycle
        try:
            self.idle_cycle()
        except Exception as e:
            audit(self.name, "idle_cycle_exception", error=str(e)[:200])

        # 3. Dream check
        try:
            self._maybe_dream()
        except Exception as e:
            audit(self.name, "dream_check_exception", error=str(e)[:200])

        # 4. Heartbeat
        heartbeat(self.name)

    def run(self):
        while True:
            try:
                self.tick()
            except Exception as e:
                audit(self.name, "tick_exception", error=str(e)[:200])
            time.sleep(self.cadence_s)

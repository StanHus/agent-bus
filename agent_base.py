# ============================================================================
# PUBLIC COPY, RELIABILITY REVISION — the loop every agent in the fabric ran
# ============================================================================
# This is no longer verbatim from `_shared/agent_base.py` (private repo
# StanHus/aptraining-mesh). It is the revision made after an external review of
# commit a4a7bbc (review/REVIEW.md, 21 contract checks in review/). The shape is
# the same: drain the inbox, handle four universal directive kinds (heal, dream,
# plan, invoke_skill) or hand off to the subclass, reply, keep a circuit
# breaker, audit, run idle work, dream twice a day, heartbeat. Subclasses still
# override handle() and idle_cycle() and call run(). What changed is that every
# failure now has one defined outcome, visible on disk under <agent>/state/:
#
#   * Per-message durable record  state/outcomes/<eid>.json walks
#     executing -> recorded -> delivered -> acknowledged. A handler runs at most
#     once per event id: the first `executing` claim is an exclusive create (a
#     second consumer that lost the race backs off) and carries the owner's pid
#     and claim time, so a consumer that meets a live claim leaves the request
#     alone instead of calling it a crash; if the reply or the acknowledgement
#     fails, the next tick redelivers or re-acknowledges from the record
#     instead of executing again. Requests are fingerprinted as the handler
#     sees them (event id from the filename, caller defaulted): a resend with
#     the same input returns the recorded result, also when the crash landed
#     between the unlink and the final record write; the same id with
#     different input is rejected. A record stuck in `executing` whose owner is
#     gone, or a record that cannot be read as one, is answered `uncertain` and
#     not re-run unless the kind is in IDEMPOTENT_KINDS (then it is re-run and
#     the record replaced). This is at-most-once, not exactly-once: see CONTRACT.md.
#   * One exception boundary for built-in and subclass handlers; results are
#     validated (dict, boolean ok, string error, JSON-serialisable) before
#     anything consumes them. Exceptions whose __str__ is broken are contained,
#     as are result values whose __str__ is broken.
#   * Failure classes: `failure` and `storage` (the agent's own state volume)
#     count toward the breaker and are counted when the handler runs, whatever
#     happens to the reply; `delivery` (one caller's outbox could not be
#     written) is audited, retried from the record and does not stop the batch
#     or count. Redelivery retries are not charged to the message budget and,
#     once a caller's outbox has failed in a tick, further redeliveries to it
#     wait for the next tick, so one unwritable caller cannot starve the others.
#   * Breaker state machine closed -> open -> half_open, monotonic clock, one
#     probe when half open (one message admitted per tick until its verdict
#     lands; a rejection probe does not let a second handler run), doubled
#     cooldown on re-trip, no open -> open edge (a failure while open does not
#     restart the cooldown). The batch stops at the threshold after the current
#     message is acknowledged; ordinary work and idle_cycle wait while open or
#     half open, `heal` directives get through, and the time spent walking past
#     paused files is not charged to the tick budget so a heal behind a backlog
#     is still reached. Every trip record and alert carries the threshold.
#   * `heal: respawn` is deferred: intent persisted, reply written, request
#     acknowledged, heartbeat written, then os._exit(0), and only once the
#     respawn's own record is acknowledged; a helper exception cannot drop the
#     exit, and a tick that finds the record acknowledged with the intent still
#     pending exits then. Boot reconciles a leftover intent (tolerating a
#     malformed file or a read-only state directory) and clears a
#     current_op.json left by a hard crash. soft_reset only removes temp files
#     older than stale_tmp_age_s.
#   * heartbeat once per tick in a finally, inside its own boundary; audit goes
#     through _audit_safe, which never raises (unprintable exceptions and
#     unserialisable fields included), keeps tracebacks and event ids, and
#     reports a diverted primary write (audit() returning False) as degraded in
#     health; run() survives audit failure. state/health.json is rewritten
#     every tick. The operator key is read without following links or
#     blocking (a FIFO or device at that path is `unreadable`).
#   * Single consumer per agent name via fcntl.flock on state/consumer.lock; a
#     lock file that cannot be opened or a volume without locking is reported
#     (consumer_lock_unavailable), not fatal; only a real contender is.
#   * Privileged kinds: the operator HMAC covers the addressed agent and the
#     args/kwargs (verified against this agent's own name), an empty or
#     unreadable key refuses every privileged request, a wrong or malformed
#     `auth` is a rejection, and invoke_skill kwargs may not shadow the
#     transport parameters `name` and `agent`.
#   * Per-tick budget (message count and elapsed time). Dream windows use
#     timedelta; a slot's attempt is persisted before the dream runs, so a dream
#     that kills the process is bounded to three runs. bd is optional (stub or absent).
#
# Local imports: `comms.py` (included), `bd` (private bead ledger; the included
# `bd.py` is a labelled no-op stub and the import is guarded, so this file also
# runs with no bd module at all) and `skill_invoker` (private, guarded).
# Some methods import `os` locally instead of using the module-level import:
# the reviewer's AST loader (review/agent_bus_regression_tests.py) and the
# repository tests replace `os` with a stub that only knows `_exit`/`getpid`.
#
# PRIVATE FILES REFERENCED BELOW:
#   OPERATING_STANDARDS.md  nine standing rules; every agent records the version
#                           it loaded, so compliance is provable, not assumed.
#   bd.py                   bead ledger; only BEAD_WORTHY_KINDS create entries.
#   skill_invoker.py        lets any agent drive any other agent's skill over the bus.
#   <agent>/NORTH_STAR.md, plans/, learnings/   per-agent state the dream sequence
#                           audits and the plan() method writes before acting.
# ============================================================================
"""_shared.agent_base — base class providing planning + dream sequence + healing receptivity.

Every macmini agent should subclass `BaseAgent`, override `handle(req) -> dict`
and optionally `idle_cycle()`, and call `run()`. Do not override `tick()`: it is
the lifecycle contract (see CONTRACT.md).

Standing rules baked in:
- Plan before non-trivial actions (PLAN.md written to state/ before action)
- Dream sequence twice daily (self-doctor: audit own state, refresh NORTH_STAR notes,
  flag anomalies)
- Healing receptivity: accepts `kind:heal` and `kind:plan` directives from peers
- Self-documentation: every decision appended to learnings/<DATE>.md
- bd integration: bead-worthy directives → bd_create; on close → bd_close
- Operating standards awareness: every agent loads _shared/OPERATING_STANDARDS.md on
  init and records `standards loaded vN` in its audit + bus_health.
"""
import errno, json, os, time, subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta, time as dtime

from comms import (
    AGENTS_ROOT, now_iso, heartbeat, audit, write_bus_health, alert_main,
    inbox_dir, outbox_dir, drain_inbox, respond, request_token, new_event_id,
    atomic_write, validate_identifier, validate_agent_name,
)

# Canonical fabric-wide standing rules. Loaded on every agent init so every
# daemon is provably aware of the standing directives (continuity mandate,
# relative-only external channels, strong-Synapse bar, real-eval recipe, chain
# discipline, resolve-items-only, no other-team mutation). See the file's header
# for the version + date. _load_operating_standards() parses the Version line.
OPERATING_STANDARDS_PATH = (
    Path(os.path.expanduser("~")) / "aptraining_agents/_shared/OPERATING_STANDARDS.md"
)

# The bead ledger is private. The public copy ships a labelled stub (bd.py); a
# checkout with no bd module at all must still import. BD_STATUS says which of
# the three cases we are in so the agent can report it instead of pretending.
try:
    import bd as _bd
    bd_create, bd_close = _bd.bd_create, _bd.bd_close
    BD_STATUS = "stub" if getattr(_bd, "BD_IS_STUB", False) else "live"
except Exception:
    def bd_create(**kw): return None
    def bd_close(bead_id, **kw): return None
    BD_STATUS = "absent"

try:
    import fcntl
except Exception:  # non-POSIX: single-consumer lock unavailable, reported at startup
    fcntl = None

# Wave 1.5 — guarded skill-reach import. A bad/missing registry or module must
# NEVER crash a daemon, so this is fully defensive: on ANY failure skill_invoker
# is None and every skill method degrades to a structured error.
try:
    import skill_invoker  # type: ignore
except Exception:
    skill_invoker = None

class BaseAgent:
    """Subclass and override `handle(req: dict) -> dict` + optionally `idle_cycle()`.

    A handler result is a dict with a boolean `ok`. On failure add a string
    `error` and, when the request itself was wrong (not the agent or its
    dependencies), `failure_class: "rejection"` so the failure does not count
    toward the circuit breaker."""
    name: str = "unnamed"
    cadence_s: int = 60
    dream_times_utc = (dtime(6, 30), dtime(18, 30))  # twice daily
    dream_catch_up = True          # run a missed slot later the same UTC day
    # Bead-noise fix 2026-06-03 (bead aptraining-7gecb): routine directive handling
    # (dreams, idle echoes, run_eval/ib.evaluate/etc.) must NOT spawn persistent task
    # beads. The audit-log (directive_handled / traced_*) is the retrace trail. ONLY
    # kinds in this allowlist get a real create+close task bead -- for genuine units
    # of work. Subclasses opt-in specific kinds. Default: none (no directive beads).
    BEAD_WORTHY_KINDS: tuple = ()
    # Kinds whose handler may safely run again when a crash left the outcome
    # record in `executing`. Everything else gets an `uncertain` reply instead.
    IDEMPOTENT_KINDS: tuple = ()
    PRIVILEGED_KINDS: tuple = ("heal", "invoke_skill")
    # Lifecycle knobs (read with getattr so the reviewer's minimal test object works).
    max_messages_per_tick: int = 50
    tick_budget_s = None           # None -> max(5, 0.8 * cadence_s)
    handler_deadline_s = None      # soft deadline: monitors compare it with the age of state/current_op.json
    breaker_cooldown_s: int = 300
    breaker_cooldown_max_s: int = 3600
    stale_tmp_age_s: int = 600     # soft_reset only removes .tmp files older than this
    outcome_retention_s: int = 7 * 24 * 3600
    claim_lease_s: int = 3600      # an `executing` claim older than this is a crash window even if its owner pid is alive

    def __init__(self):
        validate_agent_name(self.name, "agent name")
        self.self_dir = AGENTS_ROOT / self.name
        for sub in ("inbox", "outbox", "quarantine", "state", "state/outcomes",
                    "learnings", "logs", "delegations", "plans"):
            (self.self_dir / sub).mkdir(parents=True, exist_ok=True)
        self._my_skills = None  # Wave 1.5 — lazy-loaded skills.json cache
        # Circuit breaker: closed -> open -> half_open -> closed. Monotonic clock.
        self._consec_failures = 0
        self._circuit_breaker_threshold = 5
        self._circuit_breaker_pause_until = 0
        self._breaker_state = "closed"
        self._breaker_cooldown_s = self.breaker_cooldown_s
        self._breaker_reason = None
        # Lifecycle state surfaced in state/health.json.
        self._pending_exit = None
        self._in_flight = None
        self._tick_seq = 0
        self._last_progress = None
        self._last_heartbeat_written = None
        self._audit_degraded = False
        self._consumer_lock = None
        # One consumer per agent name. The kernel releases the lock on crash.
        self._acquire_consumer_lock()
        # bd: only a live ledger gets bead traffic. Stub/absent is reported once.
        self._bd_status = BD_STATUS
        self._bd_degraded = BD_STATUS != "live"
        if self._bd_degraded:
            self._audit_safe("bd_unavailable", status=BD_STATUS)
        # Operating-standards awareness — never let a missing/garbled file crash init.
        self._operating_standards_version = None
        try:
            self._load_operating_standards()
        except Exception as e:
            self._audit_safe("operating_standards_load_error", exc=e)
        # Boot reconciliation: neither a leftover file nor a read-only state/
        # may stop the daemon from coming up; each failure is audited instead.
        try:
            self._reconcile_restart_intent()
        except Exception as e:
            self._audit_safe("restart_intent_reconcile_failed", exc=e)
        try:
            self._clear_stale_current_op()
        except Exception as e:
            self._audit_safe("current_op_stale_clear_failed", exc=e)
        self._audit_safe("startup", pid=os.getpid(), cadence_s=self.cadence_s,
                         operating_standards=self._operating_standards_version, bd=BD_STATUS)

    # ---- ownership ---------------------------------------------------------
    def _acquire_consumer_lock(self):
        """Hold an exclusive flock on state/consumer.lock for the process lifetime.

        A second daemon for the same agent name fails here, loudly. The kernel
        drops the lock when the process exits or crashes, so there is nothing to
        renew and no stale lease to take over. consumer.owner.json beside it is
        for humans only."""
        lock_path = self.self_dir / "state" / "consumer.lock"
        if fcntl is None:
            self._audit_safe("consumer_lock_unavailable", reason="fcntl not available on this platform")
            return
        try:
            fh = lock_path.open("a+")
        except OSError as e:
            # A read-only state/ without a lock file, or a lock file with the
            # wrong owner: the daemon still boots (guarantee 20) and says so.
            self._audit_safe("consumer_lock_unavailable", reason="lock file cannot be opened",
                             path=lock_path.name, exc=e)
            return
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            fh.close()
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                # The lock is held: a second daemon for this agent name. Fatal, loudly.
                raise RuntimeError(f"another consumer already holds {lock_path}: {e}") from None
            # ENOLCK, ENOTSUP, EINVAL...: this volume does not do locking. Same
            # outcome as a platform without fcntl: run, report, rely on the
            # exclusive claim of the outcome record.
            self._audit_safe("consumer_lock_unavailable", reason="flock is not supported on this volume",
                             errno=e.errno, exc=e)
            return
        self._consumer_lock = fh
        try:
            atomic_write(self.self_dir / "state" / "consumer.owner.json",
                         {"agent": self.name, "pid": os.getpid(), "started": now_iso()})
        except Exception:
            pass

    def _reconcile_restart_intent(self):
        """A restart intent left in accepted/exiting means the previous process
        exited (or died) after acknowledging a respawn. Mark it completed; never
        act on it again. Called from __init__ and from the top of run()."""
        p = self.self_dir / "state" / "restart_intent.json"
        if not p.exists():
            return
        try:
            intent = json.loads(p.read_text())
        except Exception as e:
            self._audit_safe("restart_intent_corrupt", exc=e)
            return
        if not isinstance(intent, dict):
            # Valid JSON that is not an object: report it, never act on it, never crash boot.
            self._audit_safe("restart_intent_corrupt",
                             detail=f"expected a JSON object, got {type(intent).__name__}")
            return
        if intent.get("status") in ("accepted", "exiting"):
            intent["status"] = "completed"
            intent["completed_at"] = now_iso()
            try:
                tmp = p.with_name(p.name + ".tmp")
                tmp.write_text(json.dumps(intent, indent=2, default=str))
                tmp.replace(p)
            except Exception as e:
                # state/ not writable: the intent stays for a later attempt; the daemon still boots.
                self._audit_safe("restart_intent_write_failed", eid=intent.get("event_id"), exc=e)
                return
            self._audit_safe("restart_intent_reconciled", eid=intent.get("event_id"),
                             requested_by=intent.get("requested_by"))

    def _clear_stale_current_op(self):
        """state/current_op.json exists only while a handler is running. A process
        killed mid-handler (SIGKILL, OOM, power loss) leaves it behind and the
        monitor's 'operation overdue' rule would then fire for a healthy idle
        daemon. Remove it at boot and audit its age; called from __init__."""
        p = self.self_dir / "state" / "current_op.json"
        if not p.exists():
            return
        info, age_s = {}, None
        try:
            info = json.loads(p.read_text())
            if not isinstance(info, dict):
                info = {}
            started = datetime.strptime(info.get("started"), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            age_s = int((datetime.now(timezone.utc) - started).total_seconds())
        except Exception:
            pass
        try:
            p.unlink()
        except FileNotFoundError:
            return
        except Exception as e:
            self._audit_safe("current_op_stale_unremovable", eid=info.get("event_id"), exc=e)
            return
        self._audit_safe("current_op_stale_cleared", eid=info.get("event_id"), kind=info.get("kind"),
                         caller=info.get("caller"), started=info.get("started"), age_s=age_s)

    # ---- telemetry ---------------------------------------------------------
    def _safe_str(self, value, limit=500):
        """str(value)[:limit] that survives a broken __str__ (a subclass exception
        whose __str__ reads an attribute the constructor never set is a common
        bug; it must not leak through the audit or exception boundaries)."""
        try:
            return str(value)[:limit]
        except Exception:
            try:
                return repr(value)[:limit]
            except Exception:
                return f"<unprintable {type(value).__name__}>"

    def _audit_safe(self, event, exc=None, **fields):
        """audit() that cannot raise into the loop. Attaches exception type and a
        traceback (capped at 4000 chars) when exc is given; tolerates exceptions
        whose __str__ raises and fields json cannot encode. Falls back to
        <agent>/logs/audit_fallback.jsonl, then stderr. Returns True only when
        the primary audit write succeeded: audit() returning False (it never
        raises; it diverted the record to the bus fallback) counts as a
        degraded audit trail exactly like an exception would, so
        health.audit_degraded reflects the most recent audit write."""
        rec = dict(fields)
        if exc is not None:
            import traceback
            rec["error"] = self._safe_str(exc, 500)
            rec["exception_type"] = type(exc).__name__
            try:
                rec["traceback"] = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-4000:]
            except Exception:
                rec["traceback"] = None
        try:
            primary_ok = audit(self.name, event, **rec)
        except Exception as audit_error:
            primary_ok = False
            audit_note = self._safe_str(audit_error, 200)
        else:
            if primary_ok is not False:
                self._audit_degraded = False
                return True
            audit_note = "primary audit write failed; record diverted to the bus fallback"
        self._audit_degraded = True
        full = {"ts": now_iso(), "agent": self.name, "event": event, **rec, "audit_error": audit_note}
        try:
            line = json.dumps(full, default=str) + "\n"
        except Exception:
            line = json.dumps({"ts": full["ts"], "agent": self.name, "event": event,
                               "fields_repr": self._safe_str(rec, 4000),
                               "audit_error": full["audit_error"]}, default=str) + "\n"
        try:
            logs = self.self_dir / "logs"
            logs.mkdir(parents=True, exist_ok=True)
            with (logs / "audit_fallback.jsonl").open("a") as out:
                out.write(line)
        except Exception:
            try:
                import sys
                sys.stderr.write(line)
            except Exception:
                pass
        return False

    def _quarantined_count(self):
        """Quarantined requests: every entry in quarantine/ except the reason
        sidecars (an entry named <x>.reason.json whose <x> is also present). A
        request that was itself named `<x>.json.reason.json` is therefore counted."""
        qdir = self.self_dir / "quarantine"
        try:
            entries = list(qdir.iterdir()) if qdir.is_dir() else []
        except OSError:
            return None
        suffix = ".reason.json"
        return sum(1 for q in entries
                   if not (q.name.endswith(suffix) and
                           ((qdir / q.name[:-len(suffix)]).is_symlink() or (qdir / q.name[:-len(suffix)]).exists())))

    def _write_health(self):
        """state/health.json: what an operator reads first. Four independent
        signals: the heartbeat file (alive), last_progress_ts (making progress),
        breaker.state, and state/current_op.json's age against
        handler_deadline_s (current operation overdue)."""
        import os as _os
        state = self.self_dir / "state"
        state.mkdir(parents=True, exist_ok=True)
        hb = getattr(self, "_last_heartbeat_written", None)
        try:
            # null, not 0, when the inbox cannot be listed: an agent that has
            # stopped consuming must not look idle.
            inbox_pending = sum(1 for n in _os.listdir(self.self_dir / "inbox") if n.endswith(".json"))
        except OSError:
            inbox_pending = None
        key_status, _ = self._operator_key()
        health = {
            "ts": now_iso(),
            "agent": self.name,
            "alive": True,
            "tick_seq": getattr(self, "_tick_seq", 0),
            "last_progress_ts": getattr(self, "_last_progress", None),
            "breaker": {
                "state": getattr(self, "_breaker_state", "closed"),
                "until_s": max(0, round(self._circuit_breaker_pause_until - time.monotonic())),
                "consec": self._consec_failures,
                "reason": getattr(self, "_breaker_reason", None),
            },
            "current_op": getattr(self, "_in_flight", None),
            "handler_deadline_s": getattr(self, "handler_deadline_s", None),
            "restart_pending": bool(getattr(self, "_pending_exit", None)),
            "inbox_pending": inbox_pending,
            # Every quarantine entry except the reason sidecars (a sidecar is an
            # entry named <x>.reason.json whose <x> is also present).
            "quarantined_total": self._quarantined_count(),
            "audit_degraded": getattr(self, "_audit_degraded", False),
            "heartbeat_written": list(hb) if isinstance(hb, (tuple, list)) else None,
            # True only when a usable (non-empty, readable) operator key gates
            # heal/invoke_skill; `privileged_auth_key` says why when it is not.
            "privileged_auth_enforced": key_status == "ok",
            "privileged_auth_key": key_status,
            "bd": getattr(self, "_bd_status", "unknown"),
        }
        tmp = state / "health.json.tmp"
        tmp.write_text(json.dumps(health, indent=2, default=str))
        tmp.replace(state / "health.json")

    def _load_operating_standards(self):
        """Load _shared/OPERATING_STANDARDS.md into the agent's awareness.

        Records `standards loaded vN` in the audit trail and stamps the version
        into bus_health so the whole fabric is provably aware of the standing
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
        self._audit_safe("operating_standards_loaded", version=version,
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
        injecting the standing rules into an agent's LLM prompts."""
        return getattr(self, "operating_standards_text", "") or ""

    def plan(self, title, *, requirements=None, steps=None, decision_gates=None,
             failure_scenarios=None, insights=None, dependencies=None, rationale=""):
        """Write a PLAN.md following the Trilogy 'perfect plan' methodology.

        All optional parameters are keyword-only: the a4a7bbc `plan` directive
        passed them positionally and wrote rationale into the steps section.

        6 sections:
          1. Requirements Audit — every constraint logged verbatim
          2. Dependency Graph — bd-style beads with priority + blocked-by
          3. Steps — ordered action list
          4. Decision Gates — pass / adjust / abort criteria per phase
          5. Failure Scenarios — F1, F2 … with detection + cascading recovery
          6. Insight Prioritization — T1 (blocking) / T2 (significant) / T3 (incremental)

        Args:
          title: short title (also sanitised into the plan filename)
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
        safe_title = "".join(ch if (ch.isascii() and ch.isalnum()) or ch in "._-" else "_"
                             for ch in str(title))[:60].lstrip(".") or "plan"
        p = self.self_dir / "plans" / f"{ts}_{safe_title}.md"
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
                 "- Never violate the inviolable: build-upon chain / no other-team mutation / Rule 0 (no direct FW POST) / never block the operator / proactive-mesh-comms",
                 ""]
        p.write_text("\n".join(body))
        self._audit_safe("plan_written", path=p.name, steps=len(steps), gates=len(decision_gates), failures=len(failure_scenarios))
        return p

    def learn(self, kind: str, body: str):
        """Append a learning to learnings/<DATE>.md."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        f = self.self_dir / "learnings" / f"{today}.md"
        with f.open("a") as out:
            out.write(f"\n## {now_iso()} — {kind}\n\n{body}\n")
        self._audit_safe("learning_logged", kind=kind)

    def dream_sequence(self):
        """Self-doctor: audit own state + check NORTH_STAR + flag anomalies + emit summary."""
        ts = now_iso()
        ns = self.self_dir / "NORTH_STAR.md"
        ns_present = ns.exists()
        recent_audit_lines = self._count_audit_today()
        inbox_pending = len(list(inbox_dir(self.name).glob("*.json")))
        outbox_pending = len(list(outbox_dir(self.name).glob("*.json")))
        quarantined = self._quarantined_count()
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
            "quarantined": quarantined,
            "operating_standards_version": self._operating_standards_version,
        }
        self.learn("dream_sequence", json.dumps(summary, indent=2))
        self._audit_safe("dream_sequence", **summary)
        # Flag if anomalies
        if not ns_present:
            alert_main(self.name, "DREAM_NORTH_STAR_MISSING", f"# {self.name} dream: NORTH_STAR.md missing\n\n{ts}\n\nNorth Star not found in agent home. Re-creation required.\n")
        if inbox_pending > 20:
            alert_main(self.name, "DREAM_INBOX_BACKLOG", f"# {self.name} dream: inbox backlog\n\n{ts}\n\nPending: {inbox_pending} directives. Investigate stuck queue.\n")
        if quarantined:
            alert_main(self.name, "DREAM_QUARANTINE", f"# {self.name} dream: quarantined requests\n\n{ts}\n\n{quarantined} request file(s) in quarantine/. Read the .reason.json sidecars.\n")

    def _count_audit_today(self) -> int:
        from comms import AUDIT_DIR
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        f = AUDIT_DIR / f"{today}_{self.name}.jsonl"
        if not f.exists(): return 0
        try:
            with f.open() as fh:
                return sum(1 for _ in fh)
        except Exception:
            return 0

    def _load_dream_slots(self, slots_path):
        """Read state/dream_slots.json into the documented shape, repairing
        what it can: an undecodable file or a non-object resets to {} (audit
        dream_slots_corrupt); an entry that is a bare status string is
        promoted to {status, attempts}; an entry of any other wrong shape is
        dropped (audit dream_slot_entry_invalid) so it counts as no history; a
        slot left `running` means the previous process died inside the dream
        and becomes `failed` (audit dream_sequence_interrupted). Returns
        (slots, repaired) so the caller persists the repair."""
        statuses = ("pending", "running", "completed", "failed", "missed")
        repaired = False
        try:
            raw = json.loads(slots_path.read_text()) if slots_path.exists() else {}
        except Exception as e:
            self._audit_safe("dream_slots_corrupt", exc=e)
            raw, repaired = {}, True
        if not isinstance(raw, dict):
            self._audit_safe("dream_slots_corrupt", detail=f"expected an object, got {type(raw).__name__}")
            raw, repaired = {}, True
        slots = {}
        for key, value in raw.items():
            if isinstance(value, str) and value in statuses:
                value = {"status": value, "attempts": 0 if value == "pending" else 1}
                repaired = True
            if not isinstance(value, dict) or value.get("status") not in statuses:
                self._audit_safe("dream_slot_entry_invalid", slot=str(key)[:40], detail=self._safe_str(value, 100))
                repaired = True
                continue
            attempts = value.get("attempts")
            if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 0:
                value["attempts"] = 0 if value["status"] == "pending" else 1
                repaired = True
            if value["status"] == "running":
                value["status"] = "failed"
                repaired = True
                self._audit_safe("dream_sequence_interrupted", slot=key, attempt=value["attempts"])
            slots[key] = value
        return slots, repaired

    def _maybe_dream(self, now_utc=None):
        """Run the dream sequence once per scheduled UTC slot.

        A slot is `<date>T<HH:MM>` in state/dream_slots.json with status
        pending/running/completed/failed/missed and an attempt count. The
        attempt is written (status `running`) BEFORE dream_sequence runs, so a
        dream that takes the process down still counts: a slot found `running`
        at the next load is a failed attempt, and a failing or crashing dream
        retries at most three times. If the attempt cannot be persisted the
        dream does not run this tick (audit dream_slots_write_failed). The
        window is five minutes (timedelta, so 23:58 works). A slot missed while
        busy or paused is caught up once later the same UTC day when
        dream_catch_up is True; a slot never run when its day ends (or with
        catch-up off) is recorded as missed, while a slot that ran and failed
        keeps its `failed` record. `now_utc` is injectable for tests."""
        now_utc = now_utc or datetime.now(timezone.utc)
        today = now_utc.date()
        slots_path = self.self_dir / "state" / "dream_slots.json"
        slots, changed = self._load_dream_slots(slots_path)
        ran = False
        # Yesterday's slots are examined too: a window such as 23:58-00:03
        # straddles midnight, and a slot that was never run before its UTC day
        # ended must be recorded as missed (catch-up is same-day only). The walk
        # covers every day from the oldest retained slot to today (at most the
        # 7-day retention), so after downtime the slots of whole days the
        # process was not running are recorded as missed instead of leaving a
        # hole in the record; a fresh file starts at yesterday.
        first_day = today - timedelta(days=1)
        for key in slots:
            try:
                recorded_day = datetime.strptime(str(key)[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            first_day = min(first_day, recorded_day)
        first_day = max(first_day, today - timedelta(days=7))
        days = [first_day + timedelta(days=n) for n in range((today - first_day).days + 1)]
        for day in days:
            for target in self.dream_times_utc:
                window_start = datetime.combine(day, target).replace(tzinfo=timezone.utc)
                window_end = window_start + timedelta(minutes=5)
                if now_utc < window_start:
                    continue
                key = window_start.strftime("%Y-%m-%dT%H:%M")
                slot = slots.get(key) or {"status": "pending", "attempts": 0}
                if slot["status"] in ("completed", "missed") or slot["attempts"] >= 3:
                    continue
                if now_utc > window_end and not (self.dream_catch_up and day == today):
                    if slot["attempts"] == 0:
                        slot.update(status="missed", ts=now_iso())
                        slots[key] = slot
                        changed = True
                        self._audit_safe("missed_dream", slot=key)
                    # else: it ran and failed inside its window; the record stays `failed`.
                    continue
                if ran:
                    continue  # one dream per call; the next slot runs on a later tick
                # Durable attempt first: a crash inside dream_sequence must not
                # leave the slot pending with attempts 0 on disk.
                previous = dict(slot)
                slot.update(status="running", attempts=slot["attempts"] + 1, ts=now_iso(), late=now_utc > window_end)
                slots[key] = slot
                try:
                    atomic_write(slots_path, slots)
                except Exception as e:
                    self._audit_safe("dream_slots_write_failed", slot=key, exc=e)
                    slots[key] = previous
                    continue  # not run: without the record the retry bound is gone
                changed = True
                try:
                    self.dream_sequence()
                    slot["status"] = "completed"
                except Exception as e:
                    slot["status"] = "failed"
                    self._audit_safe("dream_sequence_failed", slot=key, attempt=slot["attempts"], exc=e)
                slot["ts"] = now_iso()
                ran = True
        cutoff = (now_utc - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M")
        stale = [k for k in slots if k < cutoff]
        for k in stale:
            del slots[k]
        if changed or stale:
            try:
                atomic_write(slots_path, slots)
            except Exception as e:
                self._audit_safe("dream_slots_write_failed", exc=e)

    def healing_handler(self, req: dict) -> dict:
        """Built-in handler for kind:heal — accept restart / state-reset / re-init directives.

        respawn is deferred: the intent (carrying the transport event id, which
        _dispatch fills in) is persisted and the exit happens in tick() after
        the reply is written, that request acknowledged and the heartbeat sent.
        soft_reset only removes temp files older than stale_tmp_age_s so a
        peer's in-flight write is never destroyed."""
        action = req.get("action", "soft_reset")
        if action == "soft_reset":
            stale_after = getattr(self, "stale_tmp_age_s", 600)
            now = time.time()
            removed = 0
            for k in ("inbox", "outbox"):
                for f in (self.self_dir / k).glob("*.tmp"):
                    try:
                        age = now - f.stat().st_mtime
                        if age > stale_after:
                            f.unlink()
                            removed += 1
                            self._audit_safe("self_heal_tmp_removed", path=f"{k}/{f.name}", age_s=int(age))
                    except Exception:
                        pass
            self._audit_safe("self_heal", action=action, removed=removed)
            return {"ok": True, "healed": action, "removed": removed}
        if action == "respawn":
            intent = {"event_id": req.get("event_id"), "requested_by": req.get("caller"),
                      "requested_at": now_iso(), "status": "accepted"}
            state = self.self_dir / "state"
            try:
                state.mkdir(parents=True, exist_ok=True)
                tmp = state / "restart_intent.json.tmp"
                tmp.write_text(json.dumps(intent, indent=2, default=str))
                tmp.replace(state / "restart_intent.json")
            except Exception as e:
                self._audit_safe("restart_intent_write_failed", exc=e)
                return {"ok": False, "error": f"could not persist restart intent: {str(e)[:200]}"}
            self._pending_exit = intent
            self._audit_safe("self_heal_respawn_accepted", eid=intent["event_id"], requested_by=intent["requested_by"])
            return {"ok": True, "healed": "respawn", "deferred": True}
        if action == "rewrite_north_star":
            ns = self.self_dir / "NORTH_STAR.md"
            wrote = False
            try:
                if not ns.exists():
                    ns.write_text(f"# {self.name} — NORTH_STAR\n\nRegenerated {now_iso()} by self-heal directive.\n")
                    wrote = True
            except Exception as e:
                return {"ok": False, "error": f"north star write failed: {str(e)[:200]}"}
            self._audit_safe("self_heal", action=action, wrote=wrote)
            return {"ok": True, "healed": "north_star_regen", "wrote": wrote}
        return {"ok": False, "error": f"unknown heal action {action!r}", "failure_class": "rejection"}

    def validate_directive(self, req, required=None, kinds=None):
        """Returns (ok, error_msg). Use in handler() before any work."""
        required = required or []
        for k in required:
            if k not in req or req.get(k) in (None, ""):
                return False, f"missing required field: {k}"
        if kinds is not None and req.get("kind") not in kinds:
            return False, f"unsupported kind {req.get('kind')!r}"
        return True, None

    # ---- circuit breaker ---------------------------------------------------
    def _circuit_breaker_open(self):
        """closed -> open (cooldown running) -> half_open (one probe) -> closed.
        _circuit_breaker_pause_until is a time.monotonic() deadline."""
        if time.monotonic() < self._circuit_breaker_pause_until:
            return True
        if getattr(self, "_breaker_state", "closed") == "open":
            self._breaker_state = "half_open"
            self._audit_safe("circuit_breaker_half_open", consec=self._consec_failures)
        return False

    def _trip_breaker(self, reason):
        from comms import alert_main
        base = getattr(self, "breaker_cooldown_s", 300)
        cooldown = getattr(self, "_breaker_cooldown_s", None) or base
        if getattr(self, "_breaker_state", "closed") == "half_open":
            cooldown = min(cooldown * 2, getattr(self, "breaker_cooldown_max_s", 3600))
        self._breaker_cooldown_s = cooldown
        self._breaker_state = "open"
        self._breaker_reason = str(reason)[:200]
        self._circuit_breaker_pause_until = time.monotonic() + cooldown
        # The threshold is part of every trip record and alert body, not only
        # of the reason text: a half-open probe reopening on a storage failure
        # carries a reason that names neither the count nor the threshold.
        threshold = getattr(self, "_circuit_breaker_threshold", 5)
        try:
            state = self.self_dir / "state"
            state.mkdir(parents=True, exist_ok=True)
            tmp = state / "breaker.json.tmp"
            tmp.write_text(json.dumps({"state": "open", "opened_at": now_iso(), "cooldown_s": cooldown,
                                       "consec": self._consec_failures, "threshold": threshold,
                                       "reason": self._breaker_reason},
                                      indent=2, default=str))
            tmp.replace(state / "breaker.json")
        except Exception:
            pass
        try:
            alert_main(self.name, "CIRCUIT_BREAKER_TRIPPED",
                       f"# {self.name} - circuit breaker TRIPPED\n\n**reason**: {reason}\n"
                       f"**consecutive failures**: {self._consec_failures}\n**threshold**: {threshold}\n"
                       f"**cooldown_s**: {cooldown}\n")
        except Exception as e:
            self._audit_safe("alert_main_failed", kind="CIRCUIT_BREAKER_TRIPPED", exc=e)
        self._audit_safe("circuit_breaker_tripped", reason=reason, consec=self._consec_failures,
                         threshold=threshold, cooldown_s=cooldown)

    def _count_failure(self, reason):
        """One counted failure (class `failure` or `storage`). closed: trips at
        the threshold. half_open: the probe failed, reopen with the doubled
        cooldown. open: counted but never re-tripped, so a failing heal while
        open cannot restart the cooldown or spam trip alerts (there is no
        open -> open edge in the state machine)."""
        self._consec_failures += 1
        state = getattr(self, "_breaker_state", "closed")
        if state == "half_open":
            self._trip_breaker(reason)
        elif state == "closed" and self._consec_failures >= self._circuit_breaker_threshold:
            self._trip_breaker(reason)

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
        """Override in subclass. Default returns unknown-kind error (a rejection:
        the request was wrong, the agent is healthy)."""
        return {"ok": False, "error": f"agent {self.name} has no handler for kind={req.get('kind')!r}",
                "failure_class": "rejection"}

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
            self._bd_degraded = True
            self._audit_safe("bd_degraded", reason="bd_create raised", exc=e)
            return None
        if bid is None:
            self._bd_degraded = True
            self._audit_safe("bd_degraded", reason="bd_create returned None (timeout/corrupt ledger); skipping bd-trace for process lifetime")
        return bid

    def _bd_trace_close(self, bead_id, **kw):
        """Time-bounded, non-fatal bd_close. Never blocks tick/heartbeat."""
        if not bead_id or self._bd_degraded:
            return False
        try:
            ok = bd_close(bead_id, **kw)
        except Exception as e:
            self._bd_degraded = True
            self._audit_safe("bd_degraded", reason="bd_close raised", exc=e)
            return False
        if not ok:
            self._bd_degraded = True
            self._audit_safe("bd_degraded", reason="bd_close failed (timeout/corrupt ledger); skipping bd-trace for process lifetime")
        return ok

    # ---- message lifecycle -------------------------------------------------
    def _dispatch(self, eid, req):
        """The only place a handler runs. Built-in kinds and the subclass share one
        policy: directive arguments are checked first (a bad request is a
        rejection, not a failure), privileged kinds need an HMAC `auth` field
        whenever AGENTS_ROOT/_trust/operator.key exists, and any Exception becomes
        an ok:False result with the traceback audited. BaseException propagates."""
        kind = req.get("kind") or ""
        caller = req.get("caller") or "main-a"
        # Handlers see the transport identity: the filename stem is the event id
        # (guarantee 3 allows the envelope to omit it) and caller defaults here.
        req = dict(req)
        req["event_id"] = eid
        req["caller"] = caller
        try:
            if kind in getattr(self, "PRIVILEGED_KINDS", ("heal", "invoke_skill")):
                key_status, key = self._operator_key()
                action = req.get("action") if kind == "heal" else req.get("skill")
                if key_status == "absent":
                    self._audit_safe("privileged_unauthenticated", eid=eid, kind=kind, caller=caller)
                elif key_status != "ok":
                    # An empty or unreadable key gates nothing a sender could not
                    # forge (anyone can sign with b""): refuse every privileged
                    # request until the operator fixes the key. A rejection, not
                    # a failure: the agent is healthy, the deployment is not.
                    self._audit_safe("privileged_key_unusable", eid=eid, kind=kind, caller=caller, key=key_status)
                    return {"ok": False, "error": f"operator key is {key_status}; privileged directive {kind!r} refused",
                            "failure_class": "rejection"}
                else:
                    import hmac, hashlib
                    from comms import signed_message
                    # The signature covers this agent's name and the arguments
                    # (canonical JSON, [] and {} when absent): a signed heal or
                    # invoke_skill is valid for one agent only and cannot be
                    # re-targeted by rewriting the pending file.
                    try:
                        msg = signed_message(self.name, eid, kind, action, caller,
                                             req.get("args", []), req.get("kwargs", {}))
                    except (TypeError, ValueError):
                        msg = None
                    provided = req.get("auth")
                    # compare_digest raises TypeError for non-ASCII text: a
                    # malformed `auth` is a wrong signature, not an agent failure.
                    well_formed = isinstance(provided, str) and provided.isascii()
                    expected = None if msg is None else hmac.new(key, msg, hashlib.sha256).hexdigest()
                    if expected is None or not well_formed or not hmac.compare_digest(expected, provided):
                        self._audit_safe("privileged_unauthorized", eid=eid, kind=kind, caller=caller)
                        return {"ok": False, "error": f"unauthorized privileged directive {kind!r}",
                                "failure_class": "rejection"}
            if kind == "heal":
                return self.healing_handler(req)
            if kind == "dream":
                self.dream_sequence()
                return {"ok": True, "dream": "completed"}
            if kind == "plan":
                title = req.get("title", "directed-plan")
                steps = req.get("steps", [])
                rationale = req.get("rationale", "")
                if not isinstance(title, str) or not title.strip() or len(title) > 200:
                    return {"ok": False, "error": "plan.title must be a non-empty string of at most 200 characters",
                            "failure_class": "rejection"}
                if not isinstance(steps, list) or not all(isinstance(s, str) for s in steps):
                    return {"ok": False, "error": "plan.steps must be a list of strings", "failure_class": "rejection"}
                if not isinstance(rationale, str):
                    return {"ok": False, "error": "plan.rationale must be a string", "failure_class": "rejection"}
                try:
                    # JSON may carry lone surrogates ("\udc80") that UTF-8 cannot
                    # write: text the request supplied that cannot be stored is
                    # a bad request, not an unhealthy agent.
                    for text in (title, rationale, *steps):
                        text.encode("utf-8")
                except UnicodeEncodeError:
                    return {"ok": False, "error": "plan.title, plan.steps and plan.rationale must be UTF-8 encodable text",
                            "failure_class": "rejection"}
                p = self.plan(title=title, steps=steps, rationale=rationale)
                return {"ok": True, "plan_path": str(p)}
            if kind == "invoke_skill":
                # Wave 1.5 — any agent/main-a can drive any skill over the bus.
                skill = req.get("skill")
                args = req.get("args", [])
                kwargs = req.get("kwargs", {})
                if not isinstance(skill, str) or not skill:
                    return {"ok": False, "error": "invoke_skill.skill must be a non-empty string", "failure_class": "rejection"}
                if not isinstance(args, (list, tuple)):
                    return {"ok": False, "error": "invoke_skill.args must be a list", "failure_class": "rejection"}
                if not isinstance(kwargs, dict) or not all(isinstance(k, str) for k in kwargs):
                    return {"ok": False, "error": "invoke_skill.kwargs must be an object with string keys", "failure_class": "rejection"}
                reserved = sorted(set(kwargs) & {"name", "agent"})
                if reserved:
                    # These would shadow invoke_skill's own parameter and the
                    # agent identity injected into skill_invoker.invoke.
                    return {"ok": False, "error": f"invoke_skill.kwargs must not use reserved keys {reserved}",
                            "failure_class": "rejection"}
                return self.invoke_skill(skill, *args, **kwargs)
            return self.handle(req)
        except Exception as e:
            self._audit_safe("handler_exception", eid=eid, kind=kind, caller=caller, exc=e)
            return {"ok": False, "error": f"handler raised: {self._safe_str(e, 200)}", "exception_type": type(e).__name__}

    def _validate_result(self, raw):
        """Normalise a handler result once; everything downstream (reply, breaker,
        bead, audit, record) consumes the validated form. Returns (result, class)
        where class is 'success', 'rejection' (does not count toward the breaker)
        or 'failure' (counts). A non-dict result, a non-boolean ok (the string
        "false" included), a non-string error or a result json cannot encode
        (tuple keys, for instance, or a value whose __str__ raises under
        default=str) are contract violations."""
        if not isinstance(raw, dict):
            return ({"ok": False, "error": f"handler returned {type(raw).__name__}, expected dict",
                     "failure_class": "failure"}, "failure")
        result = dict(raw)
        result.pop("event_id", None)  # transport-owned; respond() sets it
        ok = result.get("ok")
        if not isinstance(ok, bool):
            result["ok"] = False
            result["handler_error"] = self._safe_str(result.get("error"), 500) if "error" in result else None
            result["error"] = f"handler result 'ok' must be a boolean, got {type(ok).__name__}: {self._safe_str(ok, 80)}"
            result["failure_class"] = "failure"
        if "error" in result and not isinstance(result["error"], str):
            result["error"] = self._safe_str(result["error"], 500)
        try:
            json.dumps(result, default=str)
        except Exception as e:
            # The validated form is what the record and the reply carry; a result
            # that cannot be published is a contract violation, not a crash window.
            # default=str re-raises whatever a value's __str__ raises, so the
            # filter is every Exception, not only the encoder's own two.
            return ({"ok": False, "handler_ok": ok if isinstance(ok, bool) else None,
                     "error": f"handler result is not JSON-serializable: {self._safe_str(e, 200)}",
                     "failure_class": "failure"}, "failure")
        if result["ok"] is True:
            result.pop("failure_class", None)
            return result, "success"
        klass = "rejection" if result.get("failure_class") == "rejection" else "failure"
        result["failure_class"] = klass
        return result, klass

    def _operator_key(self):
        """AGENTS_ROOT/_trust/operator.key as (status, key bytes): 'absent' (no
        file: privileged kinds run unauthenticated and health says so), 'ok'
        (non-empty key), 'empty' or 'unreadable' (privileged kinds are refused
        until fixed). Whitespace around the key is ignored.

        Called on every tick (health) and for every privileged request, so it
        must never block: the entry is lstat'ed and must be a regular file of
        at most 64 KiB (a FIFO without a writer would block open(2) forever,
        a symlink or device node would read something else); it is opened
        O_NOFOLLOW|O_NONBLOCK through an fd and read to the bound. Anything
        else is 'unreadable'."""
        import os as _os, stat as _stat
        limit = 64 * 1024
        p = self.self_dir.parent / "_trust" / "operator.key"
        try:
            st = _os.lstat(str(p))
        except FileNotFoundError:
            return "absent", None
        except OSError:
            return "unreadable", None
        if not _stat.S_ISREG(st.st_mode) or st.st_size > limit:
            return "unreadable", None
        flags = _os.O_RDONLY | getattr(_os, "O_NOFOLLOW", 0) | getattr(_os, "O_NONBLOCK", 0) | getattr(_os, "O_CLOEXEC", 0)
        try:
            fd = _os.open(str(p), flags)
        except OSError:
            return "unreadable", None
        chunks, total = [], 0
        try:
            if not _stat.S_ISREG(_os.fstat(fd).st_mode):
                return "unreadable", None
            while total <= limit:
                chunk = _os.read(fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
        except OSError:
            return "unreadable", None
        finally:
            _os.close(fd)
        if total > limit:
            return "unreadable", None
        key = b"".join(chunks).strip()
        if not key:
            return "empty", None
        return "ok", key

    def _claim_is_live(self, eid, rec):
        """Is this `executing` record another consumer's claim still in progress?

        The claim carries owner {pid, claimed_at}. It is live when the claim is
        younger than claim_lease_s and its owner is alive: another pid that
        answers a signal-0 probe (POSIX only; elsewhere an owner inside the
        lease is assumed alive, which errs toward not double-executing), or
        this pid while state/current_op.json names the event id (a second
        consumer object in this process; without that file a record under our
        own pid is left over from an interrupted run). A record without owner
        data, from an older writer, is never live: guarantee 15 applies."""
        import os as _os
        owner = rec.get("owner") if isinstance(rec.get("owner"), dict) else {}
        pid = owner.get("pid")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return False
        try:
            claimed = datetime.strptime(owner.get("claimed_at"), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            age_s = (datetime.now(timezone.utc) - claimed).total_seconds()
        except Exception:
            return False
        if age_s > getattr(self, "claim_lease_s", 3600):
            return False
        if pid == _os.getpid():
            try:
                op = json.loads((self.self_dir / "state" / "current_op.json").read_text())
            except Exception:
                return False
            return isinstance(op, dict) and op.get("event_id") == eid
        if _os.name != "posix":
            return True
        try:
            _os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True   # alive, another uid
        except Exception:
            return False
        return True

    def _load_outcome(self, eid):
        """Durable record for one event id, or None when there is none. Anything
        that is not a JSON object with a recognised `status` (a truncated write,
        `null`, a list, `{}`, an unknown status) is reported as
        {'status': 'corrupt'} so the message is answered uncertain (or re-run
        when the kind is idempotent) rather than mistaken for a new message, a
        different-input duplicate or a live claim. A transient read error
        propagates (retry next tick)."""
        p = self.self_dir / "state" / "outcomes" / f"{eid}.json"
        try:
            text = p.read_text()
        except FileNotFoundError:
            return None
        try:
            rec = json.loads(text)
        except ValueError as e:
            self._audit_safe("outcome_record_corrupt", eid=eid, exc=e)
            return {"event_id": eid, "status": "corrupt"}
        if not isinstance(rec, dict) or rec.get("status") not in ("executing", "recorded", "delivered", "acknowledged"):
            self._audit_safe("outcome_record_corrupt", eid=eid,
                             detail=f"expected an object with a known status, got {self._safe_str(rec, 120)}")
            return {"event_id": eid, "status": "corrupt"}
        if rec["status"] != "executing" and rec.get("result") is not None and not isinstance(rec.get("result"), dict):
            # A recorded outcome whose result is not an object can never be
            # replayed (respond() only publishes dicts): a hand edit or a foreign
            # revision. Read it as unreadable so it is answered uncertain and
            # acknowledged instead of being retried forever as a delivery failure.
            self._audit_safe("outcome_record_corrupt", eid=eid, status=rec["status"],
                             detail=f"result must be an object, got {type(rec.get('result')).__name__}")
            return {"event_id": eid, "status": "corrupt"}
        return rec

    def _save_outcome(self, eid, rec):
        """Atomic write of state/outcomes/<eid>.json. Returns False (audited) on failure.

        The first `executing` write of an event id is the claim and is an
        exclusive create: the record is written to a private temp file and
        hard-linked into place, so two consumers cannot both believe they own
        the message. If the record already exists the claim raises
        FileExistsError (someone else owns it) instead of returning False. A
        re-execution of an idempotent kind (`reexecuted_from` set by
        _process_message: the record on disk was `executing` or unreadable) is
        not a first claim and replaces the record with tmp + rename, as does
        every later write."""
        import os as _os
        try:
            d = self.self_dir / "state" / "outcomes"
            d.mkdir(parents=True, exist_ok=True)
            rec["updated"] = now_iso()
            final = d / f"{eid}.json"
            data = json.dumps(rec, default=str)
            if rec.get("status") == "executing" and not rec.get("reexecuted_from"):
                tmp = d / f"{eid}.json.{_os.getpid()}.tmp"
                tmp.write_text(data)
                try:
                    try:
                        _os.link(str(tmp), str(final))
                    except FileExistsError:
                        raise
                    except OSError:
                        # Filesystem without hard links: exclusive create, then
                        # write. The create is the claim; if the bytes cannot be
                        # written (ENOSPC, EIO, quota) the empty or partial file
                        # is removed again, so a failed claim never leaves a
                        # record that the next tick would read as a crash window.
                        fd = _os.open(str(final), _os.O_WRONLY | _os.O_CREAT | _os.O_EXCL, 0o644)
                        try:
                            with _os.fdopen(fd, "w") as fh:
                                fh.write(data)
                        except BaseException:
                            try:
                                _os.unlink(str(final))
                            except OSError:
                                pass
                            raise
                finally:
                    try:
                        tmp.unlink()
                    except OSError:
                        pass
            else:
                tmp = d / f"{eid}.json.tmp"
                tmp.write_text(data)
                tmp.replace(final)
            return True
        except FileExistsError:
            raise
        except Exception as e:
            self._audit_safe("outcome_record_failed", eid=eid, status=rec.get("status"), exc=e)
            return False

    def _process_message(self, path, req):
        """One request through executing -> recorded -> delivered -> acknowledged.
        Returns 'continue' (handled: counts toward the message budget), 'retry'
        (only the reply or the acknowledgement of an earlier tick's outcome was
        retried: not charged to the budget), 'skip' (left untouched: another
        consumer's live claim, a lost claim race, or a redelivery deferred
        because this caller's outbox already failed this tick), 'stop' (the
        agent's own state volume failed: leave the rest of the batch) or
        'exit' (the respawn request itself reached acknowledged and wants a
        controlled shutdown).

        Failure classes seen here: `success`, `rejection`, `uncertain`,
        `failure`, `storage` (record or acknowledgement could not be written;
        counts, stops the batch) and `delivery` (this caller's outbox could not
        be written; audited, attempts.deliver bumped, retried from the record
        next tick, does not stop the batch). The breaker sees the handler's
        class when the handler runs, whatever happens to the reply; `storage`
        overrides it; `delivery` never does."""
        import hashlib, os as _os
        eid = path.stem
        kind = req.get("kind") or ""
        caller = req.get("caller") or "main-a"
        # Fingerprint what the handler sees (guarantee 7): the filename stem is
        # the event id and the caller is defaulted, so a resend that spells out
        # or omits those transport fields is the same input.
        normalised = {**req, "event_id": eid, "caller": caller}
        fingerprint = hashlib.sha256(
            json.dumps(normalised, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
        deferred = getattr(self, "_undeliverable_callers", None)
        if deferred is None:
            deferred = self._undeliverable_callers = {}
        rec = self._load_outcome(eid)
        klass = None          # None: outcome already counted on an earlier tick
        persist = True        # False: transient rejection, never overwrites the stored record
        execute = rec is None
        executed_now = False
        retry_only = False    # True: redelivery / re-acknowledgement of an earlier tick's outcome
        if rec is not None:
            status = rec.get("status")
            if status == "executing" and self._claim_is_live(eid, rec):
                # Another consumer is inside the handler right now (no fcntl, or
                # a second daemon around the lock). Not a crash window: no
                # uncertain reply, no alert, no acknowledgement; its owner finishes.
                self._audit_safe("claim_in_progress", eid=eid, kind=kind, caller=caller, owner=rec.get("owner"))
                return "skip"
            if status != "corrupt" and rec.get("fingerprint") != fingerprint:
                self._audit_safe("identity_reuse", eid=eid, kind=kind, caller=caller,
                                 recorded=rec.get("fingerprint"), received=fingerprint)
                rec = {"status": "recorded", "result": {
                    "ok": False, "error": "event_id reused with different input; original outcome kept",
                    "failure_class": "rejection", "kind": kind}}
                persist = False
                klass = "rejection"
            elif status in ("executing", "corrupt"):
                if kind in getattr(self, "IDEMPOTENT_KINDS", ()):
                    self._audit_safe("reexecuting_idempotent", eid=eid, kind=kind, previous_status=status)
                    rec["reexecuted_from"] = status   # not a first claim: replace the record, do not create it
                    execute = True
                else:
                    rec.setdefault("fingerprint", fingerprint)
                    rec.setdefault("attempts", {"execute": 0, "deliver": 0, "ack": 0})
                    rec.update(status="recorded", outcome_class="uncertain", kind=kind, caller=caller, result={
                        "ok": False, "outcome": "uncertain", "status": "uncertain", "kind": kind,
                        "error": "execution interrupted before its outcome was recorded; not re-run",
                        "failure_class": "uncertain"})
                    self._save_outcome(eid, rec)
                    self._audit_safe("outcome_uncertain", eid=eid, kind=kind, caller=caller, previous_status=status)
                    try:
                        from comms import alert_main
                        alert_main(self.name, "OUTCOME_UNCERTAIN",
                                   f"# {self.name} - uncertain outcome\n\nevent_id: {eid}\nkind: {kind}\ncaller: {caller}\n\n"
                                   f"The previous process stopped between running this handler and recording its outcome. "
                                   f"Reconcile downstream before resubmitting.\n")
                    except Exception as e:
                        self._audit_safe("alert_main_failed", kind="OUTCOME_UNCERTAIN", exc=e)
                    klass = "uncertain"
            elif status == "acknowledged":
                # Duplicate resend of a completed request: replay the recorded result.
                self._audit_safe("duplicate_replayed", eid=eid, kind=kind, caller=caller, previous_status=status)
                rec["status"] = "recorded"
            elif status == "delivered":
                # Either the acknowledgement failed last tick (the reply is still in
                # the caller's outbox: only the unlink is retried, guarantee 13) or
                # the process died after the unlink and before the final record
                # write and the caller, having consumed the reply, resent the same
                # input: then the reply is gone and must be republished. "Present"
                # means the recorded reply, not any file under that name: a
                # different document in the slot (the rejection of a same-id
                # misuse, or something the caller wrote) is not the reply, and
                # the republish overwrites it atomically.
                try:
                    from comms import outbox_dir
                    published = json.loads((outbox_dir(caller) / f"{eid}.json").read_text())
                    recorded = json.loads(json.dumps({**(rec.get("result") or {}), "event_id": eid}, default=str))
                    reply_present = published == recorded
                except Exception:
                    reply_present = False
                if not reply_present:
                    self._audit_safe("duplicate_replayed", eid=eid, kind=kind, caller=caller, previous_status=status)
                    rec["status"] = "recorded"
                else:
                    retry_only = True
            elif status == "recorded":
                retry_only = True
                if caller in deferred:
                    # This caller's outbox already failed this tick: one attempt
                    # per caller per tick, the rest of the batch gets the time.
                    deferred[caller] += 1
                    if deferred[caller] == 1:
                        self._audit_safe("redelivery_deferred", eid=eid, kind=kind, caller=caller)
                    return "skip"
        if execute:
            if rec is None:
                rec = {"event_id": eid, "fingerprint": fingerprint, "kind": kind, "caller": caller,
                       "received": now_iso(), "attempts": {"execute": 0, "deliver": 0, "ack": 0}}
            rec.update(status="executing", fingerprint=fingerprint, kind=kind, caller=caller,
                       owner={"pid": _os.getpid(), "claimed_at": now_iso()})
            attempts = rec.get("attempts")
            if not isinstance(attempts, dict):
                attempts = rec["attempts"] = {"execute": 0, "deliver": 0, "ack": 0}   # corrupt or foreign record
            attempts["execute"] = attempts.get("execute", 0) + 1
            try:
                claimed = self._save_outcome(eid, rec)
            except FileExistsError:
                # Another consumer claimed this event id between our load and our
                # write. Not ours: leave the file; the next tick finds its record.
                self._audit_safe("claim_conflict", eid=eid, kind=kind, caller=caller)
                return "skip"
            if not claimed:
                # Without the record the dedup guarantee is gone: do not execute, leave the batch.
                self._count_failure("outcome record could not be written")
                return "stop"
            bead_id = None
            if kind in self.BEAD_WORTHY_KINDS:
                bead_id = self._bd_trace_create(
                    title=f"[{self.name}] {kind} from {caller}",
                    priority=2,
                    description=f"event_id={eid}\nkind={kind}\ncaller={caller}\nreceived={now_iso()}",
                    labels=[f"agent:{self.name}", f"kind:{kind}"],
                )
            op = {"event_id": eid, "kind": kind, "caller": caller, "started": now_iso(),
                  "deadline_s": getattr(self, "handler_deadline_s", None)}
            self._in_flight = op
            op_path = self.self_dir / "state" / "current_op.json"
            try:
                op_path.write_text(json.dumps(op, default=str))
            except Exception as e:
                # The handler still runs (the claim landed), but the monitor's
                # 'operation overdue' signal is off for this message and the
                # audit trail has to say so, as it does for a failed removal.
                self._audit_safe("current_op_write_failed", eid=eid, kind=kind, exc=e)
            try:
                result, klass = self._validate_result(self._dispatch(eid, req))
            finally:
                self._in_flight = None
                try:
                    op_path.unlink()
                except FileNotFoundError:
                    pass
                except Exception as e:
                    # The monitor's 'operation overdue' rule will fire on this
                    # file; the audit trail must say why it is still there.
                    self._audit_safe("current_op_unremovable", eid=eid, kind=kind, exc=e)
            executed_now = True
            rec.update(status="recorded", result=result, outcome_class=klass)
            if not self._save_outcome(eid, rec):
                klass = "storage"   # reply anyway; the breaker must notice records are not being kept
            if bead_id:
                if klass == "success":
                    self._bd_trace_close(bead_id, reason="completed", note=json.dumps({"caller": caller, "kind": kind, "result_ok": True})[:200])
                else:
                    self._bd_trace_close(bead_id, reason="failed", note=self._safe_str(result.get("error", "unknown"), 200))
        result = rec.get("result") or {"ok": False, "error": "no recorded result", "failure_class": "failure"}
        stop = False
        if rec.get("status") == "recorded":
            try:
                respond(caller, eid, result)
                rec["status"] = "delivered"
                if persist and not self._save_outcome(eid, rec):
                    klass = "storage"   # the reply is out; the state volume is what failed
            except Exception as e:
                # This caller's outbox, not the agent's state volume: keep the
                # recorded result, retry the reply next tick, let the batch go
                # on. The class the handler earned this tick stands: a failing
                # probe still reopens the breaker, a fifth failure still trips
                # it, a successful probe still closes it.
                if persist:
                    rec.setdefault("attempts", {})["deliver"] = rec["attempts"].get("deliver", 0) + 1
                    self._save_outcome(eid, rec)
                deferred.setdefault(caller, 0)
                self._audit_safe("response_delivery_failed", eid=eid, kind=kind, caller=caller,
                                 attempts=rec.get("attempts", {}).get("deliver"), outcome_class=klass, exc=e)
        if rec.get("status") == "delivered":
            try:
                try:
                    path.unlink()
                except FileNotFoundError:
                    # Already gone (moved out by hand, or removed by another consumer): acknowledged.
                    self._audit_safe("ack_already_gone", eid=eid, kind=kind)
                rec["status"] = "acknowledged"
                if persist and not self._save_outcome(eid, rec):
                    klass = "storage"
                self._last_progress = now_iso()
            except Exception as e:
                if persist:
                    rec.setdefault("attempts", {})["ack"] = rec["attempts"].get("ack", 0) + 1
                    self._save_outcome(eid, rec)
                self._audit_safe("ack_failed", eid=eid, kind=kind, exc=e)
                klass, stop = "storage", True
        # Breaker bookkeeping: success closes a half-open breaker; rejection and
        # uncertain never count; failure and storage count and may trip (closed
        # at the threshold, half_open on the first counted failure). A reply
        # that could not be delivered changes none of this.
        if klass == "success":
            self._consec_failures = 0
            if getattr(self, "_breaker_state", "closed") == "half_open":
                self._breaker_state = "closed"
                self._breaker_cooldown_s = getattr(self, "breaker_cooldown_s", 300)
                self._breaker_reason = None
                self._audit_safe("circuit_breaker_closed")
        elif klass in ("failure", "storage"):
            self._count_failure(f"{self._circuit_breaker_threshold}+ consecutive failures "
                                f"(last: {klass}: {self._safe_str(result.get('error', '?'), 100)})")
        if klass is not None:
            self._audit_safe("directive_handled", eid=eid, kind=kind, caller=caller,
                             ok=result.get("ok") is True, outcome_class=klass, status=rec.get("status"))
        if stop:
            return "stop"
        pending = getattr(self, "_pending_exit", None)
        # Only the respawn request's OWN durable record reaching acknowledged
        # exits: not another message, and not the transient rejection record of
        # a different-input duplicate carrying the same event id.
        if pending and persist and rec.get("status") == "acknowledged" and pending.get("event_id") == eid:
            return "exit"
        if retry_only and not executed_now and klass is None:
            return "retry"
        return "continue"

    def _controlled_exit(self):
        """Last step of a deferred respawn: the reply is written, the request is
        gone and the heartbeat is sent, so a supervisor restart will not meet the
        same request again. Marks the intent `exiting`, then os._exit(0)."""
        intent = dict(getattr(self, "_pending_exit", None) or {})
        intent["status"] = "exiting"
        intent["exiting_at"] = now_iso()
        try:
            state = self.self_dir / "state"
            tmp = state / "restart_intent.json.tmp"
            tmp.write_text(json.dumps(intent, indent=2, default=str))
            tmp.replace(state / "restart_intent.json")
        except Exception as e:
            self._audit_safe("restart_intent_write_failed", exc=e)
        self._audit_safe("self_heal_respawn_exiting", eid=intent.get("event_id"))
        os._exit(0)

    def tick(self):
        """One pass of the lifecycle. Read this first; CONTRACT.md explains each step.

        1. Drain the inbox within a message/time budget. Each request goes through
           _process_message (record, dispatch, reply, acknowledge). When the
           breaker is open only `heal` requests are taken; the rest stay put and
           the time spent walking past them is not charged to the budget, so a
           heal behind a paused backlog is still reached. While half open one
           message is admitted per tick as the probe; the rest of the batch waits
           (heal excepted) until its verdict closes or reopens the breaker.
           Redelivery retries of earlier outcomes are not charged to the message
           budget (the time budget still bounds them), and a caller whose outbox
           failed once this tick is not retried again until the next tick.
        2. idle_cycle (skipped while the breaker is open or half open), then the dream check.
        3. Always: heartbeat exactly once (a helper exception is audited, never
           fatal), then write state/health.json.
        4. If the respawn request itself was acknowledged (this tick, or on an
           earlier tick whose exit did not happen), exit now, after the heartbeat.
        """
        exit_after = False
        self._tick_seq = getattr(self, "_tick_seq", 0) + 1
        try:
            pending = getattr(self, "_pending_exit", None)
            if pending and pending.get("event_id"):
                # An accepted respawn whose durable record already reached
                # acknowledged (a helper failure on that tick skipped the exit):
                # nothing can return 'exit' for it again, so carry it here.
                try:
                    done = self._load_outcome(pending["event_id"])
                except Exception:
                    done = None
                if isinstance(done, dict) and done.get("status") == "acknowledged":
                    exit_after = True
            try:
                started = time.monotonic()
                budget_n = getattr(self, "max_messages_per_tick", 50)
                budget_s = getattr(self, "tick_budget_s", None) or max(5.0, 0.8 * (getattr(self, "cadence_s", 0) or 0))
                processed = 0
                paused_since = None      # clock reading at which a paused file was passed over
                probe_admitted = False   # half open: one message per tick until a verdict lands
                self._undeliverable_callers = {}   # callers whose outbox failed this tick
                for path, req in (() if exit_after else drain_inbox(self.name)):
                    now = time.monotonic()
                    if paused_since is not None:
                        # Walking past a paused file did no work: refund its time
                        # so a backlog ahead of a heal cannot exhaust the budget.
                        started += now - paused_since
                        paused_since = None
                    if processed >= budget_n or now - started > budget_s:
                        self._audit_safe("tick_budget_exhausted", processed=processed,
                                         remaining=len(list((self.self_dir / "inbox").glob("*.json"))))
                        break
                    kind = req.get("kind") or ""
                    if self._circuit_breaker_open() and kind != "heal":
                        paused_since = now
                        continue  # paused: ordinary work waits in the inbox, recovery gets through
                    half_open = getattr(self, "_breaker_state", "closed") == "half_open"
                    if half_open and probe_admitted and kind != "heal":
                        paused_since = now
                        continue  # the probe is in; its verdict decides the rest of the batch
                    try:
                        outcome = self._process_message(path, req)
                    except Exception as e:
                        processed += 1
                        probe_admitted = probe_admitted or half_open
                        self._audit_safe("message_exception", eid=path.stem, exc=e)
                        continue
                    if outcome == "exit":
                        exit_after = True
                        break
                    if outcome == "stop":
                        break
                    if outcome not in ("retry", "skip"):
                        processed += 1   # handled work; retries and back-offs are not charged
                        probe_admitted = probe_admitted or half_open
            except Exception as e:
                self._audit_safe("inbox_exception", exc=e)
            if not exit_after:
                # idle_cycle waits while the breaker is open AND while it is half
                # open: half open admits one message probe, not unbounded idle work,
                # and idle outcomes cannot close the breaker.
                if not self._circuit_breaker_open() and getattr(self, "_breaker_state", "closed") == "closed":
                    try:
                        self.idle_cycle()
                    except Exception as e:
                        self._audit_safe("idle_cycle_exception", exc=e)
                try:
                    self._maybe_dream()
                except Exception as e:
                    self._audit_safe("dream_check_exception", exc=e)
        finally:
            try:
                try:
                    self._last_heartbeat_written = heartbeat(self.name)
                except Exception as e:
                    # The helper swallows its own errors; if one escapes anyway
                    # it must not decide the tick's outcome (or a deferred exit).
                    self._last_heartbeat_written = (False, False)
                    self._audit_safe("heartbeat_failed", exc=e)
            finally:
                try:
                    self._write_health()
                except Exception as e:
                    self._audit_safe("health_write_failed", exc=e)
        if exit_after:
            self._controlled_exit()

    def run(self):
        try:
            self._reconcile_restart_intent()
        except Exception as e:
            self._audit_safe("restart_intent_reconcile_failed", exc=e)
        while True:
            try:
                self.tick()
            except Exception as e:
                self._audit_safe("tick_exception", exc=e)
            time.sleep(self.cadence_s)

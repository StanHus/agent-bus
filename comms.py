# ============================================================================
# PUBLIC COPY — the bus that ran a 56-daemon agent fabric (June–Aug 2026)
# ============================================================================
# Verbatim from the fabric's shared module (`_shared/comms.py`, private repo
# StanHus/aptraining-mesh). Only this comment block is new.
#
# WHAT IT DID. 56 long-running agents on a Mac mini (curators, evaluators, a
# training-chain conductor, a token manager, a mesh doctor, a continuity watchdog)
# talked to each other through this file and nothing else. No broker, no ports,
# no queue service, no dependencies. Two months, 16,083 messages surfaced to the
# operator, 47 agents heartbeating at any time. A laptop saw the same bus because
# the directory tree lived in iCloud.
#
# HOW. A message is a JSON file. Delivery is `write .tmp` then `rename()`, which
# is atomic on POSIX: a reader sees a whole message or none. A reply is the same
# file name in the caller's outbox. `send_directive` is therefore synchronous RPC
# with correlation ids and a timeout, in 22 lines. Chosen by measurement, not
# taste (header below: p95 0.402 ms, p99 0.508 ms, 100 % success, 50/50 crash
# survival in the 2026-06-02 shootout against the alternatives).
#
# FILES IT TALKS TO (private, not in this copy):
#   agent_base.py      BaseAgent.tick() drains the inbox via drain_inbox() and
#                      answers via respond(); every daemon subclasses it. Included here.
#   token_manager.py   the only holder of credentials (macOS Keychain); agents ask
#                      for a token with request_token() over the bus, never via env.
#   mesh_doctor.py     reads _bus_health/ and _audit/ written by write_bus_health()
#                      and audit() below; heartbeat() feeds the liveness dashboard.
#   OPERATING_STANDARDS.md  the fabric's constitution; agent_base loads it on boot.
# ============================================================================

def atomic_write(path, content):
    """Write content to path atomically via .tmp + rename. Use for state files, configs, etc.
    Defeats partial-write corruption on crash mid-write."""
    from pathlib import Path
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if isinstance(content, (dict, list)):
        import json as _json
        tmp.write_text(_json.dumps(content, indent=2, default=str))
    else:
        tmp.write_text(str(content))
    tmp.replace(path)

"""_shared.comms — canonical localfs_poll inbox/outbox helper.

Winner of comms shootout 2026-06-02: p95 0.402ms / p99 0.508ms / 100% success / 50/50 crash-survival.
All macmini agents should import from here instead of re-implementing inbox/outbox handling.

Inbox files MUST be valid JSON with {event_id, kind, caller, ...}.
Outbox files written atomically (write to .tmp, rename).
Idempotent on transient FS errors; logs but does not raise to caller.
"""
import json, os, time, uuid
from pathlib import Path
from datetime import datetime, timezone

AGENTS_ROOT = Path(os.path.expanduser("~")) / "aptraining_agents"
ICLOUD = Path(os.path.expanduser("~")) / "Library/Mobile Documents/com~apple~CloudDocs/work/aptraining"
COMMS = ICLOUD / "workspace/comms"
HB_DIR = COMMS / "_heartbeats"
LOCAL_HB_DIR = AGENTS_ROOT / "_heartbeats_local"  # TCC-free local path; launchd can write here
BH_DIR = COMMS / "_bus_health"
AUDIT_DIR = COMMS / "_audit"
TOMAIN = COMMS / "to_main"

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def new_event_id(prefix="evt"):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"

def heartbeat(agent_name: str):
    # LOCAL write first: TCC-free under launchd, real freshness source of truth.
    try:
        LOCAL_HB_DIR.mkdir(parents=True, exist_ok=True)
        _lp = LOCAL_HB_DIR / f"{agent_name}.heartbeat"
        _tmp = _lp.with_suffix(".heartbeat.tmp")
        _tmp.write_text(now_iso())
        _tmp.replace(_lp)  # atomic
    except Exception:
        pass  # never crash the tick on heartbeat write
    # iCLOUD write too (legacy; visible cross-host when synced / from interactive shell).
    try:
        HB_DIR.mkdir(parents=True, exist_ok=True)
        (HB_DIR / f"{agent_name}.heartbeat").touch()
    except Exception:
        pass

def audit(agent_name: str, event: str, **kwargs):
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    f = AUDIT_DIR / f"{today}_{agent_name}.jsonl"
    rec = {"ts": now_iso(), "agent": agent_name, "event": event, **kwargs}
    with f.open("a") as out:
        out.write(json.dumps(rec, default=str) + "\n")

def write_bus_health(agent_name: str, payload: dict):
    BH_DIR.mkdir(parents=True, exist_ok=True)
    f = BH_DIR / f"{agent_name}_summary.json"
    tmp = f.with_suffix(".tmp")
    tmp.write_text(json.dumps({"ts": now_iso(), "agent": agent_name, **payload}, indent=2, default=str))
    tmp.replace(f)

def alert_main(agent_name: str, kind: str, body: str):
    """Drop an alert to to_main/."""
    TOMAIN.mkdir(parents=True, exist_ok=True)
    ts = now_iso().replace(":", "")
    f = TOMAIN / f"{ts}_{kind}_{agent_name}.md"
    f.write_text(body)
    audit(agent_name, "alerted_main", kind=kind, file=f.name)

def inbox_dir(agent_name: str) -> Path:
    p = AGENTS_ROOT / agent_name / "inbox"
    p.mkdir(parents=True, exist_ok=True)
    return p

def outbox_dir(agent_name: str) -> Path:
    p = AGENTS_ROOT / agent_name / "outbox"
    p.mkdir(parents=True, exist_ok=True)
    return p

def drain_inbox(agent_name: str):
    """Yield (path, parsed_dict) for each json file in inbox. Caller MUST unlink after handling."""
    for f in sorted(inbox_dir(agent_name).glob("*.json")):
        try:
            yield f, json.loads(f.read_text())
        except Exception as e:
            audit(agent_name, "inbox_malformed", path=str(f), error=str(e)[:80])
            try: f.unlink()
            except Exception: pass

def respond(caller: str, event_id: str, payload: dict):
    """Write a response file to the caller's outbox atomically."""
    out = outbox_dir(caller)
    target = out / f"{event_id}.json"
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps({"event_id": event_id, **payload}, default=str))
    tmp.replace(target)

def send_directive(target_agent: str, payload: dict, caller: str = "main-a", timeout_s: int = 180):
    """Send a directive to target agent's inbox; block for response in caller's outbox up to timeout_s."""
    eid = payload.get("event_id") or new_event_id()
    payload = {**payload, "event_id": eid, "caller": caller}
    inbox = inbox_dir(target_agent)
    target = inbox / f"{eid}.json"
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, default=str))
    tmp.replace(target)
    response_path = outbox_dir(caller) / f"{eid}.json"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if response_path.exists():
            try:
                resp = json.loads(response_path.read_text())
                response_path.unlink()
                return resp
            except Exception:
                return None
        time.sleep(0.5)
    return None

def request_token(self_name: str, token_name: str, timeout_s: int = 30):
    """Convenience wrapper: ask token-manager for a token."""
    resp = send_directive(
        target_agent="token-manager",
        payload={"request": "get_token", "token_name": token_name},
        caller=self_name,
        timeout_s=timeout_s,
    )
    if resp and resp.get("ok"):
        return resp.get("value")
    return None

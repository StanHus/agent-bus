# ============================================================================
# PUBLIC COPY, RELIABILITY REVISION — the bus that ran a 56-daemon agent fabric
# ============================================================================
# This is no longer verbatim from the fabric's `_shared/comms.py` (private repo
# StanHus/aptraining-mesh). It is the revision made after an external review of
# commit a4a7bbc (review/REVIEW.md). The transport is unchanged: a message is a
# JSON file, delivered by `write .tmp` then `rename()`. What changed is what the
# bus guarantees about bad input and about failure:
#
#   * Agent names and event ids are bounded identifiers, never paths.
#     validate_identifier() runs before any mkdir or write; a caller or event id
#     containing a separator or traversal raises ValueError and creates nothing.
#     Agent names (callers, targets) additionally may not start with `_`: that
#     prefix is reserved for bus files beside the agent directories
#     (_registry.json, _trust, _audit_fallback, _heartbeats_local).
#   * Agent directories and their inbox/outbox must be real directories under
#     AGENTS_ROOT; a symbolic link in either position is refused, so a caller
#     cannot redirect replies out of the tree.
#   * respond() keeps the transport event id authoritative: a payload cannot
#     overwrite it. The temp file is `<eid>.json.tmp` in the same outbox and is
#     created exclusively (O_EXCL | O_NOFOLLOW): a pre-planted link under that
#     name is removed as a directory entry, never written through. When a
#     registry exists, replies to unlisted callers are refused.
#   * drain_inbox() never deletes. Only regular files are opened: the size is
#     taken from lstat before anything is read, the read itself is bounded, and
#     a symlink, FIFO, device or directory named like a request is quarantined
#     (`not_a_regular_file`) instead of followed. A transient read error leaves
#     the file for the next tick (audit inbox_read_error); an inbox directory
#     that cannot be listed drains nothing and audits inbox_unlistable instead
#     of looking empty. An undecodable file (a JSON error, a bad UTF-8 byte or
#     a document nested too deep to decode alike), a schema-invalid file, or
#     one from a caller missing from _registry.json, is moved byte-for-byte
#     into `<agent>/quarantine/` with a `.reason.json` sidecar (audit
#     inbox_quarantined). The sidecar name is reserved with an exclusive create
#     before the move, so a quarantined file is never overwritten by a later
#     sidecar or a same-second collision; a name too long to take the sidecar
#     suffix is quarantined under a bounded `<prefix>~<hash>` name with the
#     original recorded in the sidecar. The filename stem is the authoritative
#     event id; an envelope event_id that disagrees is quarantined.
#   * audit() never raises, even for fields json cannot encode. Primary
#     destination, then `AGENTS_ROOT/_audit_fallback/`, then stderr. Returns
#     True only when the primary write succeeded.
#   * heartbeat() reports what it managed to write: (local_ok, icloud_ok); an
#     invalid agent name writes nothing. write_bus_health() and alert_main()
#     validate the agent name too.
#   * send_directive() validates target, caller and event id, honours an
#     optional `AGENTS_ROOT/_registry.json` list of known agents, and creates
#     its temp file exclusively (O_EXCL | O_NOFOLLOW) like respond() does: a
#     `<eid>.json.tmp` link planted in the target inbox by another sender is
#     removed as a directory entry, never written through.
#   * sign_directive() covers the addressed agent, event_id, kind, action,
#     caller and the canonical args/kwargs (one canonical JSON array, so no
#     field can absorb a separator). The receiving agent verifies against its
#     own name: a signed heal or invoke_skill is valid for exactly one agent
#     and cannot be copied into another inbox or re-targeted in transit.
#
# The contracts and the tests that pin them are listed in CONTRACT.md.
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
# with correlation ids and a timeout. Chosen by measurement, not taste (p95
# 0.402 ms, p99 0.508 ms, 100 % success, 50/50 crash survival in the 2026-06-02
# shootout against the alternatives).
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
    tmp = path.with_name(path.name + ".tmp")
    if isinstance(content, (dict, list)):
        import json as _json
        tmp.write_text(_json.dumps(content, indent=2, default=str))
    else:
        tmp.write_text(str(content))
    tmp.replace(path)

"""_shared.comms — canonical localfs_poll inbox/outbox helper.

Winner of comms shootout 2026-06-02: p95 0.402ms / p99 0.508ms / 100% success / 50/50 crash-survival.
All macmini agents should import from here instead of re-implementing inbox/outbox handling.

Inbox files MUST be regular files holding a JSON object, named `<event_id>.json`. If the
object carries an `event_id` it must equal the filename stem. `caller` (default "main-a") must
be an agent name (an identifier not starting with `_`), listed in `AGENTS_ROOT/_registry.json`
when that file exists; `kind`, when present, is a string of at most 64 characters.
Outbox files, and requests sent with send_directive(), are written atomically
(exclusive-create .tmp, rename). drain_inbox() never deletes a message: the
consumer acknowledges by unlinking.

Several functions import `os` and `stat` locally instead of using the module-level
import: the reviewer's AST loader (review/agent_bus_regression_tests.py) executes them
in a namespace whose `os` is a stub, and local imports keep those checks runnable.
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

MAX_MESSAGE_BYTES = 1024 * 1024  # a request larger than this is quarantined, not parsed
RESERVED_AGENT_PREFIX = "_"      # names beside the agent directories: _registry.json, _trust, ...

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def new_event_id(prefix="evt"):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"

def validate_identifier(value, what="identifier", max_len=128):
    """Agent names and event ids are identifiers, not paths.

    Accepts a non-empty str of at most max_len characters drawn from
    [A-Za-z0-9._-] that does not start with '.'. Everything else (separators,
    '..', empty, oversized, non-str) raises ValueError before any filesystem use.
    Returns the value unchanged so it can be used inline."""
    if not isinstance(value, str) or not value or len(value) > max_len:
        raise ValueError(f"{what} must be a non-empty string of at most {max_len} characters, got {value!r}")
    if value.startswith("."):
        raise ValueError(f"{what} must not start with '.', got {value!r}")
    for ch in value:
        if not ((ch.isascii() and ch.isalnum()) or ch in "._-"):
            raise ValueError(f"{what} contains a forbidden character {ch!r}: {value!r}")
    return value

def validate_agent_name(value, what="agent name"):
    """An agent name (own name, caller, target) is an identifier that does not
    start with '_'. The underscore prefix is reserved for the bus's own files
    beside the agent directories (_registry.json, _trust/, _audit_fallback/,
    _heartbeats_local/); a caller named `_registry.json` would otherwise turn
    the registry into a directory and break send_directive fabric-wide."""
    validate_identifier(value, what)
    if value.startswith("_"):
        raise ValueError(f"{what} must not start with '_' (reserved for bus files under AGENTS_ROOT), got {value!r}")
    return value

def agent_subdir(agent_name: str, sub: str) -> Path:
    """AGENTS_ROOT/<agent>/<sub>, created if missing. Both the agent directory
    and the subdirectory must be real directories: a symbolic link in either
    position would let whoever controls it redirect requests or replies outside
    AGENTS_ROOT, so it raises PermissionError and nothing is written."""
    validate_agent_name(agent_name)
    base = AGENTS_ROOT / agent_name
    p = base / sub
    for q in (base, p):
        if q.is_symlink():
            raise PermissionError(f"{q} is a symbolic link; agent directories must be real directories under {AGENTS_ROOT}")
    p.mkdir(parents=True, exist_ok=True)
    if base.is_symlink() or p.is_symlink() or not p.is_dir():
        raise PermissionError(f"{p} is not a real directory under {AGENTS_ROOT}")
    return p

def heartbeat(agent_name: str):
    """Touch the liveness files. Returns (local_written, icloud_written) so a
    caller can tell an attempted heartbeat from a delivered one. Never raises;
    an invalid agent name writes nothing and returns (False, False)."""
    local_ok = icloud_ok = False
    try:
        validate_agent_name(agent_name)
    except ValueError:
        return local_ok, icloud_ok
    # LOCAL write first: TCC-free under launchd, real freshness source of truth.
    try:
        LOCAL_HB_DIR.mkdir(parents=True, exist_ok=True)
        _lp = LOCAL_HB_DIR / f"{agent_name}.heartbeat"
        _tmp = LOCAL_HB_DIR / f"{agent_name}.heartbeat.tmp"
        _tmp.write_text(now_iso())
        _tmp.replace(_lp)  # atomic
        local_ok = True
    except Exception:
        pass  # never crash the tick on heartbeat write
    # iCLOUD write too (legacy; visible cross-host when synced / from interactive shell).
    try:
        HB_DIR.mkdir(parents=True, exist_ok=True)
        (HB_DIR / f"{agent_name}.heartbeat").touch()
        icloud_ok = True
    except Exception:
        pass
    return local_ok, icloud_ok

def audit(agent_name: str, event: str, **kwargs):
    """Append one JSON line to today's audit file. Never raises.

    Primary: AUDIT_DIR/<date>_<agent>.jsonl. If that fails the record goes to
    AGENTS_ROOT/_audit_fallback/<date>_<agent>.jsonl with the primary error
    attached; if that also fails, one line to stderr. Fields json cannot encode
    (tuple keys, circular structures) are written as their repr under
    `fields_repr` with `audit_serialization_error`. An agent name that is not an
    identifier is filed under `_invalid-agent` with the raw value kept in the
    record. Returns True only when the primary write succeeded, so callers can
    surface a degraded audit trail."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        file_stem = validate_identifier(agent_name, "agent name")
    except ValueError:
        file_stem = "_invalid-agent"
    rec = {"ts": now_iso(), "agent": agent_name, "event": event, **kwargs}
    if file_stem != agent_name:
        rec["agent"] = repr(agent_name)[:200]
    try:
        line = json.dumps(rec, default=str) + "\n"
    except Exception as bad:
        try:
            fields = repr(kwargs)[:4000]
        except Exception:
            fields = "<unrepresentable>"
        rec = {"ts": rec["ts"], "agent": rec["agent"], "event": event,
               "fields_repr": fields, "audit_serialization_error": str(bad)[:200]}
        line = json.dumps(rec, default=str) + "\n"
    try:
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        with (AUDIT_DIR / f"{today}_{file_stem}.jsonl").open("a") as out:
            out.write(line)
        return True
    except Exception as primary:
        try:
            fallback = AGENTS_ROOT / "_audit_fallback"
            fallback.mkdir(parents=True, exist_ok=True)
            rec["audit_primary_error"] = str(primary)[:200]
            with (fallback / f"{today}_{file_stem}.jsonl").open("a") as out:
                out.write(json.dumps(rec, default=str) + "\n")
        except Exception:
            try:
                import sys
                sys.stderr.write(line)
            except Exception:
                pass
        return False

def write_bus_health(agent_name: str, payload: dict):
    validate_agent_name(agent_name)
    BH_DIR.mkdir(parents=True, exist_ok=True)
    f = BH_DIR / f"{agent_name}_summary.json"
    tmp = BH_DIR / f"{agent_name}_summary.json.tmp"
    tmp.write_text(json.dumps({"ts": now_iso(), "agent": agent_name, **payload}, indent=2, default=str))
    tmp.replace(f)

def alert_main(agent_name: str, kind: str, body: str):
    """Drop an alert to to_main/."""
    validate_agent_name(agent_name)
    safe_kind = "".join(ch if (ch.isascii() and ch.isalnum()) or ch in "._-" else "_" for ch in str(kind))[:64] or "ALERT"
    TOMAIN.mkdir(parents=True, exist_ok=True)
    ts = now_iso().replace(":", "")
    f = TOMAIN / f"{ts}_{safe_kind}_{agent_name}.md"
    f.write_text(body)
    audit(agent_name, "alerted_main", kind=kind, file=f.name)

def inbox_dir(agent_name: str) -> Path:
    return agent_subdir(agent_name, "inbox")

def outbox_dir(agent_name: str) -> Path:
    return agent_subdir(agent_name, "outbox")

def read_message(path):
    """Decode boundary. Returns (obj, None) on success, else (None, (code, detail)).

    code is 'read_error' (transient filesystem failure: the caller must leave
    the file alone), or bad input the caller quarantines: 'not_a_regular_file'
    (symlink, FIFO, device, directory: never opened, so it cannot redirect the
    read or block it), 'oversized' (size taken from lstat before anything is
    read; the read itself stops at the limit) or 'decode_error'. The file is
    never modified here."""
    import os as _os, stat as _stat
    limit = 1024 * 1024  # MAX_MESSAGE_BYTES; literal so the function is self-contained
    try:
        st = _os.lstat(path)
    except OSError as e:
        return None, ("read_error", str(e)[:200])
    if not _stat.S_ISREG(st.st_mode):
        what = ("symbolic link" if _stat.S_ISLNK(st.st_mode) else "fifo" if _stat.S_ISFIFO(st.st_mode)
                else "directory" if _stat.S_ISDIR(st.st_mode) else "special file")
        return None, ("not_a_regular_file", f"inbox entry is a {what}, not a regular file")
    if st.st_size > limit:
        return None, ("oversized", f"{st.st_size} bytes")
    flags = _os.O_RDONLY | getattr(_os, "O_NOFOLLOW", 0) | getattr(_os, "O_NONBLOCK", 0) | getattr(_os, "O_CLOEXEC", 0)
    try:
        fd = _os.open(path, flags)
    except OSError as e:
        return None, ("read_error", str(e)[:200])
    chunks, total = [], 0
    try:
        if not _stat.S_ISREG(_os.fstat(fd).st_mode):
            return None, ("not_a_regular_file", "inbox entry changed type between lstat and open")
        while True:
            chunk = _os.read(fd, min(65536, limit + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > limit:
                return None, ("oversized", f"more than {limit} bytes")
    except OSError as e:
        return None, ("read_error", str(e)[:200])
    finally:
        _os.close(fd)
    raw = b"".join(chunks)
    try:
        return json.loads(raw.decode("utf-8")), None
    except ValueError as e:  # JSONDecodeError and UnicodeDecodeError
        return None, ("decode_error", str(e)[:200])
    except RecursionError:
        # A document nested deeper than the decoder can follow fits under the
        # size cap; it is bad input, not a reason to stop the batch.
        return None, ("decode_error", "JSON nested too deep to decode")

def quarantine_message(agent_name: str, path, code: str, detail: str):
    """Move a bad request out of the inbox, bytes untouched, for diagnosis.

    Destination: AGENTS_ROOT/<agent>/quarantine/<original name>, or on collision
    `<name>.<timestamp>` then `<name>.<timestamp>.<n>`, plus a `<name>.reason.json`
    sidecar carrying the reason. The sidecar is created exclusively BEFORE the
    move, so its name reserves the slot: a quarantined request that happens to
    be named `<x>.json.reason.json`, or a second collision in the same second,
    can never be overwritten. An original name too long to carry the sidecar
    suffix (NAME_MAX is 255 on most filesystems) is quarantined under the
    bounded name `<first 180 chars>~<sha256 of the name, 16 hex>` and the
    sidecar's `original` field keeps the full name; the same fallback is used
    when the filesystem itself reports the name too long. Symlinks and FIFOs
    are moved as directory entries (rename), never opened. Returns the new
    path, or None if the move failed (the file then stays in the inbox and is
    retried next tick; audited either way)."""
    import os as _os, errno as _errno, hashlib as _hashlib
    qdir = AGENTS_ROOT / agent_name / "quarantine"
    reason = {"ts": now_iso(), "agent": agent_name, "original": path.name,
              "reason_code": code, "detail": detail}
    flags = _os.O_WRONLY | _os.O_CREAT | _os.O_EXCL | getattr(_os, "O_NOFOLLOW", 0)
    bounded = f"{path.name[:180]}~{_hashlib.sha256(path.name.encode('utf-8', 'surrogateescape')).hexdigest()[:16]}"
    bases = [bounded] if len(path.name) > 200 else [path.name, bounded]
    try:
        qdir.mkdir(parents=True, exist_ok=True)
        stamp = now_iso().replace(":", "")
        target = sidecar = None
        for base in bases:
            too_long = False
            for n in range(0, 1000):
                name = base if n == 0 else f"{base}.{stamp}" if n == 1 else f"{base}.{stamp}.{n - 1}"
                t, s = qdir / name, qdir / f"{name}.reason.json"
                if _os.path.lexists(t):
                    continue
                try:
                    fd = _os.open(str(s), flags, 0o644)
                except FileExistsError:
                    continue
                except OSError as e:
                    if e.errno == _errno.ENAMETOOLONG and base != bounded:
                        too_long = True   # this volume has a shorter NAME_MAX: try the bounded name
                        break
                    raise
                with _os.fdopen(fd, "w") as fh:
                    fh.write(json.dumps({**reason, "quarantined_as": name}, indent=2, default=str))
                target, sidecar = t, s
                break
            if target is not None or not too_long:
                break
        if target is None:
            raise OSError(f"no free quarantine name for {path.name}")
        try:
            path.replace(target)
        except OSError:
            try:
                sidecar.unlink()
            except OSError:
                pass
            raise
    except OSError as e:
        audit(agent_name, "inbox_quarantine_failed", path=path.name, reason=code, error=str(e)[:200])
        return None
    audit(agent_name, "inbox_quarantined", path=target.name, reason=code, detail=str(detail)[:200])
    return target

def drain_inbox(agent_name: str):
    """Yield (path, request) for each valid request file in the inbox, sorted by name.

    Never deletes. A read error leaves the file for the next tick. A non-regular
    entry, an undecodable, oversized or schema-invalid file, or a request whose
    caller is missing from _registry.json (when that file exists) goes to
    quarantine/. A valid request is a JSON object whose filename stem is an
    identifier; its optional event_id must equal that stem, its caller (default
    main-a) must be an agent name and its kind, if present, a string of at most
    64 characters. When the registry exists but cannot be read, nothing is
    drained this pass (audit registry_unreadable); when the inbox directory
    itself cannot be listed, nothing is drained either and inbox_unlistable is
    audited, so a permission or I/O problem on the directory never looks like
    an empty inbox. The consumer acknowledges a handled request by unlinking
    its path."""
    import os as _os
    inbox = inbox_dir(agent_name)
    try:
        registry = known_agents()
    except Exception as e:
        audit(agent_name, "registry_unreadable", error=str(e)[:200])
        return
    try:
        names = _os.listdir(inbox)
    except OSError as e:
        audit(agent_name, "inbox_unlistable", path=str(inbox), error=str(e)[:200])
        return
    for name in sorted(names):
        if not name.endswith(".json"):
            continue
        f = inbox / name
        obj, err = read_message(f)
        if err is not None:
            code, detail = err
            if code == "read_error":
                audit(agent_name, "inbox_read_error", path=f.name, error=detail)
                continue
            quarantine_message(agent_name, f, code, detail)
            continue
        problem = None
        if not isinstance(obj, dict):
            problem = ("not_an_object", f"top-level JSON is {type(obj).__name__}, expected object")
        else:
            try:
                validate_identifier(f.stem, "event_id (filename)")
                if "event_id" in obj and obj["event_id"] != f.stem:
                    problem = ("event_id_mismatch", f"envelope event_id {obj['event_id']!r} != filename {f.stem!r}")
                else:
                    caller = validate_agent_name(obj.get("caller", "main-a"), "caller")
                    kind = obj.get("kind", "")
                    if not isinstance(kind, str) or len(kind) > 64:
                        problem = ("bad_kind", f"kind must be a string of at most 64 characters, got {kind!r}"[:200])
                    elif registry is not None and caller not in registry:
                        problem = ("unregistered_caller", f"caller {caller!r} is not listed in _registry.json")
            except ValueError as e:
                problem = ("bad_identifier", str(e))
        if problem is not None:
            quarantine_message(agent_name, f, *problem)
            continue
        yield f, obj

def respond(caller: str, event_id: str, payload: dict):
    """Write a response file to the caller's outbox atomically.

    caller and event_id are validated before anything touches the filesystem;
    when _registry.json exists the caller must be listed. The transport
    event_id is authoritative: a payload event_id never survives. The temp file
    `<eid>.json.tmp` is created exclusively (O_EXCL | O_NOFOLLOW): if an entry
    already exists under that name (a leftover from an interrupted reply, or a
    link planted by the caller) it is removed as a directory entry and the
    create is retried once, so the agent never writes through someone else's
    link. Raises OSError when the outbox cannot be written."""
    import os as _os
    validate_agent_name(caller, "caller")
    validate_identifier(event_id, "event_id")
    if not isinstance(payload, dict):
        raise TypeError(f"response payload must be a dict, got {type(payload).__name__}")
    registry = known_agents()
    if registry is not None and caller not in registry:
        raise ValueError(f"caller {caller!r} is not in {AGENTS_ROOT / '_registry.json'}; reply refused")
    out = outbox_dir(caller)
    target = out / f"{event_id}.json"
    tmp = out / f"{event_id}.json.tmp"
    data = json.dumps({**payload, "event_id": event_id}, default=str).encode("utf-8")
    flags = _os.O_WRONLY | _os.O_CREAT | _os.O_EXCL | getattr(_os, "O_NOFOLLOW", 0) | getattr(_os, "O_CLOEXEC", 0)
    fd = None
    for attempt in (0, 1):
        try:
            fd = _os.open(str(tmp), flags, 0o644)
            break
        except FileExistsError:
            if attempt:
                raise
            _os.unlink(str(tmp))  # removes the entry (or the link itself), never its target
    with _os.fdopen(fd, "wb") as fh:
        fh.write(data)
    _os.replace(str(tmp), str(target))

def signed_message(target, event_id, kind, action, caller, args=None, kwargs=None) -> bytes:
    """The bytes an operator signature covers: one canonical JSON array
    `[target, event_id, kind, action, caller, args, kwargs]` (sorted keys, no
    whitespace), so no field can absorb a separator and shift the meaning of
    another. `target` is the agent the directive is addressed to; the receiving
    agent substitutes its own name when verifying. `action` is the heal action
    or the skill name; `args` defaults to [] and `kwargs` to {}. Shared by
    sign_directive() and BaseAgent._dispatch()."""
    return json.dumps([target or "", event_id, kind, action or "", caller,
                       [] if args is None else args, {} if kwargs is None else kwargs],
                      sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")

def sign_directive(key: bytes, event_id: str, kind: str, action: str, caller: str, args=None, kwargs=None,
                   target=None) -> str:
    """HMAC-SHA256 over signed_message(target, event_id, kind, action, caller,
    args, kwargs). Put the result in the request's `auth` field. BaseAgent
    requires it for privileged kinds (heal, invoke_skill) whenever
    AGENTS_ROOT/_trust/operator.key exists and verifies it against its OWN
    name as the target, so a signature is valid for exactly one agent: the same
    signed bytes copied into another agent's inbox are rejected there. Always
    pass `target`; a signature made without one verifies nowhere (it fails
    closed rather than everywhere). Because the arguments are covered too, a
    signed invoke_skill cannot be re-targeted by rewriting the pending file."""
    import hmac, hashlib
    return hmac.new(key, signed_message(target, event_id, kind, action, caller, args, kwargs),
                    hashlib.sha256).hexdigest()

def known_agents():
    """Optional registry: AGENTS_ROOT/_registry.json holding a list of agent
    names (or an object keyed by name). Returns None when there is no registry;
    raises ValueError when the file exists but is not a readable list/object."""
    p = AGENTS_ROOT / "_registry.json"
    if not p.exists():
        return None
    if not p.is_file():
        raise ValueError(f"{p} exists but is not a file")
    try:
        data = json.loads(p.read_text())
    except ValueError as e:
        raise ValueError(f"{p} is not valid JSON: {str(e)[:100]}") from None
    if isinstance(data, dict):
        data = list(data.keys())
    if not isinstance(data, list):
        raise ValueError(f"{p} must hold a list of agent names or an object keyed by name")
    return {str(n) for n in data}

def send_directive(target_agent: str, payload: dict, caller: str = "main-a", timeout_s: int = 180):
    """Send a directive to target agent's inbox; block for response in caller's outbox up to timeout_s.

    Raises ValueError for an invalid target, caller or event id, or for a target
    missing from _registry.json when a registry exists. The temp file
    `<eid>.json.tmp` is created exclusively (O_EXCL | O_NOFOLLOW): every sender
    can write into the target inbox, so an entry already under that name (a
    link planted by another sender, or a leftover) is removed as a directory
    entry and the create retried once; the request body is never written
    through someone else's link. Raises OSError when the inbox cannot be
    written."""
    import os as _os
    validate_agent_name(target_agent, "target agent")
    validate_agent_name(caller, "caller")
    registry = known_agents()
    if registry is not None and target_agent not in registry:
        raise ValueError(f"target agent {target_agent!r} is not in {AGENTS_ROOT / '_registry.json'}")
    eid = validate_identifier(payload.get("event_id") or new_event_id(), "event_id")
    payload = {**payload, "event_id": eid, "caller": caller}
    inbox = inbox_dir(target_agent)
    target = inbox / f"{eid}.json"
    tmp = inbox / f"{eid}.json.tmp"
    data = json.dumps(payload, default=str).encode("utf-8")
    flags = _os.O_WRONLY | _os.O_CREAT | _os.O_EXCL | getattr(_os, "O_NOFOLLOW", 0) | getattr(_os, "O_CLOEXEC", 0)
    fd = None
    for attempt in (0, 1):
        try:
            fd = _os.open(str(tmp), flags, 0o644)
            break
        except FileExistsError:
            if attempt:
                raise
            _os.unlink(str(tmp))  # removes the entry (or the link itself), never its target
    with _os.fdopen(fd, "wb") as fh:
        fh.write(data)
    _os.replace(str(tmp), str(target))
    response_path = outbox_dir(caller) / f"{eid}.json"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if response_path.exists():
            try:
                resp = json.loads(response_path.read_text())
            except Exception:
                return None
            try:
                response_path.unlink()
            except OSError:
                pass
            return resp
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
    if resp and resp.get("ok") is True:
        return resp.get("value")
    return None

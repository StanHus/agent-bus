"""STUB — not part of the fabric. The real `bd.py` is the fabric's bead ledger (private).
agent_base.py imports bd_create/bd_close through a guarded import and reports BD_STATUS
"stub" when it finds this file (BD_IS_STUB below), "absent" when there is no bd module and
"live" otherwise; only a live ledger receives bead traffic. This no-op stub exists so the
public copy runs standalone. In the fabric, bd_create/bd_close write to the shared task ledger."""
BD_IS_STUB = True
def bd_create(**kw): return None
def bd_close(bead_id, **kw): return None
def bd_note(bead_id, **kw): return None
def bd_status(*a, **kw): return None

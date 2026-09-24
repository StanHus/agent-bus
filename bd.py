"""STUB — not part of the fabric. The real `bd.py` is the fabric's bead ledger (private).
agent_base.py hard-imports these four names; this no-op stub exists only so the public copy
imports and runs standalone. In the fabric, bd_create/bd_close write to the shared task ledger."""
def bd_create(**kw): return None
def bd_close(bead_id, **kw): return None
def bd_note(bead_id, **kw): return None
def bd_status(*a, **kw): return None

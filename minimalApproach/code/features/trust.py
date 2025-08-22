from __future__ import annotations
from typing import Dict, Any

def compute_trust_v0(row: Dict[str, Any]) -> float:
    """
    trust_v0 = 0.6*participation - 0.35*miss_rate - 0.05*slashed_flag
    """
    att_total = row.get("attestations_total") or 0
    miss_att = row.get("att_missed_total") or 0
    denom = att_total + miss_att
    participation = (att_total / denom) if denom > 0 else 0.0
    miss_rate = (miss_att / denom) if denom > 0 else 0.0
    slashed = 1 if row.get("slashed") else 0
    trust = 0.6 * participation - 0.35 * miss_rate - 0.05 * slashed
    return round(float(trust), 4)

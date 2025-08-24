from __future__ import annotations
from typing import Dict, Any

def compute_trust_v0(row: Dict[str, Any]) -> float:
    """
    trust_v0 = 0.6*participation - 0.35*miss_rate - 0.05*slashed_flag
    """
    def parse_slashed(value):
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return 1 if value != 0 else 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes"}:
                return 1
            if normalized in {"0", "false", "no", ""}:
                return 0
        return 0

    att_total = row.get("attestations_total") or 0
    miss_att = row.get("att_missed_total") or 0
    denom = att_total + miss_att
    participation = (att_total / denom) if denom > 0 else 0.0
    miss_rate = (miss_att / denom) if denom > 0 else 0.0
    slashed = parse_slashed(row.get("slashed"))
    trust = 0.6 * participation - 0.35 * miss_rate - 0.05 * slashed
    return round(float(trust), 4)

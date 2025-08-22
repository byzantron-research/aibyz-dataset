from __future__ import annotations
from typing import Dict, Any, List
from ..beaconchain import get_validator_overview, get_validator_performance
from ..http import HttpClient

def collect_validator_rows(client: HttpClient, indexes: List[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx in indexes:
        ov = get_validator_overview(client, idx)
        pf = get_validator_performance(client, idx)
        rows.append({**ov, **pf})
    return rows

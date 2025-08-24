from __future__ import annotations
from typing import Dict, Any, List
from ..beaconchain import get_validator_overview, get_validator_performance
from ..http import HttpClient
import time

def collect_validator_rows(client: HttpClient, indexes: List[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx in indexes:
        try:
            ov = get_validator_overview(client, idx)
            time.sleep(client.delay)
            pf = get_validator_performance(client, idx)
            rows.append({**ov, **pf})
        except Exception as err:
            rows.append({"index": idx, "error": str(err)})
            continue
    return rows

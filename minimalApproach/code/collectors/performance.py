from __future__ import annotations
from typing import Dict, Any, List
from beaconchain import get_validator_overview, get_validator_performance
from http_client import HttpClient
import requests.exceptions
import sys
import time

def collect_validator_rows(client: HttpClient, indexes: List[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx in indexes:
        try:
            ov = get_validator_overview(client, idx)
            time.sleep(client.rate_limit_seconds)
            pf = get_validator_performance(client, idx)
            rows.append({**ov, **pf})
        except (requests.exceptions.RequestException, ValueError) as err:
            print(f"[WARN] Failed to process index {idx}: {err}", file=sys.stderr)
            continue
        except Exception as err:
            print(f"[ERROR] Unexpected error for index {idx}: {err}", file=sys.stderr)
            continue
    return rows

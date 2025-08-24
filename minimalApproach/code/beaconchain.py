from __future__ import annotations
from typing import Dict, Any
from .http import HttpClient

# Beaconcha.in /api/v1 endpoints relevant for validator collection

def get_validator_overview(client: HttpClient, index: int) -> Dict[str, Any]:
    data = client.get_json(f"validator/{index}")
    d = data.get("data", {}) if isinstance(data, dict) else {}
    return {
        "validator_index": index,
        "pubkey": d.get("pubkey"),
        "status": d.get("status"),
        "effective_balance_gwei": d.get("effectivebalance"),
        "slashed": d.get("slashed"),
        "activation_epoch": d.get("activationepoch"),
        "exit_epoch": d.get("exitepoch"),
        "withdrawal_credentials": d.get("withdrawalcredentials"),
    }

def get_validator_performance(client: HttpClient, index: int) -> Dict[str, Any]:
    out = {"validator_index": index}
    perf = client.get_json(f"validator/{index}/performance")
    p = perf.get("data", {}) if isinstance(perf, dict) else {}
    if isinstance(p, list):
        p = p[0] if p else {}
    out.update({
        "attestations_total": p.get("attestationscount"),
        "att_missed_total": p.get("missedattestations"),
        "proposals_total": p.get("proposalscount"),
        "prop_missed_total": p.get("missedproposals"),
        "inclusion_delay_avg": p.get("inclusiondelay"),
        "rewards_sum_gwei": p.get("sumrewards"),
    })
    return out

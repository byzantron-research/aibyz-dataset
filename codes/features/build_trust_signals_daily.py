"""Feature generation for trust signals on validators.

This script computes simple trust signals for validators on a daily basis.
Currently it measures the effective balance as a proxy trust score and
counts the number of penalties incurred by each validator. The output is
written to a partitioned directory under the ``features`` layer.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict
import pandas as pd
from common.storage import read_any, part_path, write_rows

def build_trust_signals_daily(cfg: Dict[str, str], date: str) -> None:
    """Compute daily trust signals for validators and persist them to disk.

    Currently the trust score is a direct mapping from the ``effective_balance``
    field. A count of penalties incurred per validator is also emitted.

    :param cfg: Chain configuration dictionary containing at least
      ``chain_id`` and ``network``, and optionally ``root`` and ``format``.
    :param date: The date partition (``YYYY‑MM‑DD``) to process.
    """
    chain_id = cfg["chain_id"]
    network = cfg["network"]
    root = Path(cfg.get("root", "data"))
    fmt = cfg.get("format", "parquet")
    vc = read_any(root, "curated", "validator_core", chain_id, network, date)
    pc = read_any(root, "curated", "penalty_core", chain_id, network, date)
    rows: list = []
    if not vc.empty:
        slash_counts: Dict[str, int] = {}
        if not pc.empty and "validator_id" in pc.columns:
            # Crude: count penalties per validator (if available)
            slash_counts = (
                pc.groupby("validator_id")["penalty_type"].count().to_dict()
            )
        for _, r in vc.iterrows():
            vid = r["validator_id"]
            eff_bal = r.get("effective_balance")
            rows.append(
                {
                    "chain_id": chain_id,
                    "network": network,
                    "date": date,
                    "validator_id": vid,
                    "trust_score_v0": None
                    if pd.isna(eff_bal)
                    else float(eff_bal or 0.0),
                    "penalty_count_v0": int(slash_counts.get(vid, 0)),
                }
            )
    out = part_path(root, "features", "trust_signals_daily", chain_id, network, date)
    write_rows(rows, out, fmt)

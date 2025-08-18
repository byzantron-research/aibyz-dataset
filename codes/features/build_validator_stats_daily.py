"""Feature generation for validator statistics.

This script defines a helper function to build daily statistics about
validators, such as counts and average balances, as well as counts of
blocks. The resulting dataset is written to a partitioned directory under
the ``features`` layer.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict
import pandas as pd
from common.storage import part_path, read_any, write_rows

def build_validator_stats_daily(cfg: Dict[str, str], date: str) -> None:
    """Compute daily validator statistics and persist them to disk.

    The function reads curated ``validator_core`` and ``block_core`` tables
    for a given chain and network, computes summary statistics such as the
    number of unique validators, average balances and number of blocks, and
    writes the results as a single partition to the ``features`` layer.

    :param cfg: Chain configuration dictionary containing at least
      ``chain_id`` and ``network``, and optionally ``root`` and ``format``.
    :param date: The date partition (``YYYY‑MM‑DD``) to process.
    """
    chain_id = cfg["chain_id"]
    network = cfg["network"]
    root = Path(cfg.get("root", "data"))
    fmt = cfg.get("format", "parquet")

    vc = read_any(root, "curated", "validator_core", chain_id, network, date)
    bc = read_any(root, "curated", "block_core", chain_id, network, date)

    rows: list = []
    if not vc.empty:
        rows.append(
            {
                "chain_id": chain_id,
                "network": network,
                "date": date,
                "num_validators": int(vc["validator_id"].nunique()),
                "avg_balance": (
                    vc["balance"].mean() if "balance" in vc.columns else None
                ),
                "avg_effective_balance": (
                    vc["effective_balance"].mean()
                    if "effective_balance" in vc.columns
                    else None
                ),
            }
        )
    if not bc.empty:
        rows.append(
            {
                "chain_id": chain_id,
                "network": network,
                "date": date,
                "num_blocks": int(bc["height_or_slot"].nunique()),
            }
        )
    out = part_path(root, "features", "validator_stats_daily", chain_id, network, date)
    write_rows(rows, out, fmt)

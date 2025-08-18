import argparse
import logging
import yaml
from datetime import datetime, timezone
from typing import Dict

from collectors.eth2 import Eth2Collector
from collectors.cosmos import CosmosCollector
from collectors.polkadot import PolkadotCollector

def load_cfg(path: str) -> Dict[str, any]:
    """Load a YAML configuration file and return it as a dictionary."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_chain_cfg(root_cfg: Dict[str, any], chain_id: str) -> Dict[str, any]:
    """Extract and merge chain‑specific configuration from the root config.

    The returned configuration inherits top‑level ``format`` and ``root`` keys
    from the root config if they are present.

    :param root_cfg: The root configuration dictionary parsed from YAML.
    :param chain_id: Identifier of the desired chain (e.g. ``"eth2"``).
    :returns: A dictionary representing the configuration for the requested chain.
    :raises SystemExit: If the chain_id is unknown.
    """
    for c in root_cfg.get("chains", []):
        if c["chain_id"] == chain_id:
            cc = dict(c)
            cc["format"] = root_cfg.get("format", "parquet")
            cc["root"] = root_cfg.get("root", "data")
            return cc
    raise SystemExit(f"Unknown chain_id: {chain_id}")

def get_collector(chain_id: str, cfg: Dict[str, any]):
    """Instantiate the appropriate collector for the given chain."""
    if chain_id == "eth2":
        print (cfg)
        return Eth2Collector(cfg)
    if chain_id == "cosmos":
        return CosmosCollector(cfg)
    if chain_id == "polkadot":
        return PolkadotCollector(cfg)
    raise SystemExit(f"No collector for chain_id={chain_id}")

def today_str() -> str:
    """Return the current date in ``YYYY‑MM‑DD`` format (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def main() -> None:
    """Entry point for the hybrid dataset construction CLI."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="Hybrid Dataset Construction CLI")
    p.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    sub = p.add_subparsers(dest="cmd", required=True)
    # Collect subcommand
    c = sub.add_parser("collect", help="Ingest raw data from chain APIs")
    c.add_argument("--chain", required=True, choices=["eth2", "cosmos", "polkadot"])
    c.add_argument("--date", default=today_str(), help="Ingest date (YYYY-MM-DD)")
    c.add_argument(
        "--datasets",
        default="blocks,validators,attestations,penalties",
        help="Comma-separated datasets to collect",
    )
    c.add_argument("--start-slot", type=int, help="[eth2] start slot (inclusive)")
    c.add_argument("--end-slot", type=int, help="[eth2] end slot (inclusive)")
    c.add_argument("--lookback-slots", type=int, help="[eth2] last N slots")
    c.add_argument("--start-height", type=int, help="[cosmos/polkadot] start height")
    c.add_argument("--end-height", type=int, help="[cosmos/polkadot] end height")
    c.add_argument("--lookback-blocks", type=int, help="[cosmos/polkadot] last N blocks")
    # Curate subcommand
    d = sub.add_parser("curate", help="Transform raw → curated core tables")
    d.add_argument("--chain", required=True, choices=["eth2", "cosmos", "polkadot"])
    d.add_argument("--date", default=today_str())
    # Features subcommand
    e = sub.add_parser(
        "features", help="Build daily features from curated tables"
    )
    e.add_argument("--chain", required=True, choices=["eth2", "cosmos", "polkadot"])
    e.add_argument("--date", default=today_str())
    args = p.parse_args()
    cfg = load_cfg(args.config)
    chain_cfg = get_chain_cfg(cfg, args.chain)
    if args.cmd == "collect":
        collector = get_collector(args.chain, chain_cfg)
        datasets = [x.strip() for x in args.datasets.split(",") if x.strip()]
        kw: Dict[str, any] = dict(datasets=datasets, ingest_date=args.date)
        for k in [
            "start_slot",
            "end_slot",
            "lookback_slots",
            "start_height",
            "end_height",
            "lookback_blocks",
        ]:
            if getattr(args, k, None) is not None:
                kw[k] = getattr(args, k)
        collector.collect(**kw)
        return
    if args.cmd == "curate":
        from curators.common import Curator
        Curator(chain_cfg).curate(ingest_date=args.date)
        return
    if args.cmd == "features":
        from features.build_validator_stats_daily import (
            build_validator_stats_daily,
        )
        from features.build_trust_signals_daily import (
            build_trust_signals_daily,
        )
        build_validator_stats_daily(chain_cfg, date=args.date)
        build_trust_signals_daily(chain_cfg, date=args.date)
        return

if __name__ == "__main__":
    main()

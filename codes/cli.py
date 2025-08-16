import argparse
import sys
from pathlib import Path
import yaml

from common.utils import today_str
from collectors.eth2 import Eth2Collector
from collectors.cosmos import CosmosCollector
from collectors.polkadot import PolkadotCollector
from curators.common import Curator
from features.build_validator_stats_daily import build_validator_stats_daily
from features.build_trust_signals_daily import build_trust_signals_daily


def load_cfg(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_chain_cfg(root_cfg: dict, chain_id: str):
    for c in root_cfg.get("chains", []):
        if c["chain_id"] == chain_id:
            c = dict(c)  # shallow copy
            c["format"] = root_cfg.get("format", "parquet")
            c["root"] = root_cfg.get("root", ".")
            return c
    raise SystemExit(f"Unknown chain_id: {chain_id}")

def get_collector(chain_id: str, cfg: dict):
    if chain_id == "eth2":
        from collectors.eth2 import Eth2Collector
        return Eth2Collector(cfg)
    if chain_id == "cosmos":
        from collectors.cosmos import CosmosCollector
        return CosmosCollector(cfg)
    if chain_id == "polkadot":
        try:
            from collectors.polkadot import PolkadotCollector
        except ImportError as e:
            raise SystemExit(
                "Polkadot requires 'substrate-interface'. Install it first: pip install substrate-interface"
            ) from e
        return PolkadotCollector(cfg)
    raise SystemExit(f"Unsupported chain_id {chain_id}")

def main():
    p = argparse.ArgumentParser("dataCollection")
    p.add_argument("--cfg", default=str(Path(__file__).with_name("config.yaml")))
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("collect", help="Collect raw data")
    c.add_argument("--chain", required=True, choices=["eth2", "cosmos", "polkadot"])
    c.add_argument("--datasets", default="blocks,validators,attestations,penalties")
    c.add_argument("--start", type=int)
    c.add_argument("--end", type=int)
    c.add_argument("--limit", type=int)
    c.add_argument("--date", default=today_str())

    u = sub.add_parser("curate", help="Normalize raw → curated")
    u.add_argument("--chain", required=True, choices=["eth2", "cosmos", "polkadot"])
    u.add_argument("--date", default=today_str())

    f = sub.add_parser("features", help="Build features")
    f.add_argument("--chain", required=True, choices=["eth2", "cosmos", "polkadot"])
    f.add_argument("--date", default=today_str())

    args = p.parse_args()
    cfg = load_cfg(args.cfg)
    chain_cfg = get_chain_cfg(cfg, args.chain)

    if args.cmd == "collect":
        from common.utils import today_str as _  # keep import local
        collector = get_collector(chain_cfg["chain_id"], chain_cfg)
        datasets = [x.strip() for x in args.datasets.split(",") if x.strip()]
        collector.collect(datasets=datasets, start=args.start, end=args.end, limit=args.limit, ingest_date=args.date)
        return

    if args.cmd == "curate":
        from curators.common import Curator
        Curator(chain_cfg).curate(ingest_date=args.date)
        return

    if args.cmd == "features":
        from features.build_validator_stats_daily import build_validator_stats_daily
        from features.build_trust_signals_daily import build_trust_signals_daily
        build_validator_stats_daily(chain_cfg, date=args.date)
        build_trust_signals_daily(chain_cfg, date=args.date)
        return

if __name__ == "__main__":
    main()

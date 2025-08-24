from __future__ import annotations
import argparse
import sys
from pathlib import Path

from eth_dataset.config import (
    get_api_base,
    get_api_key,
    get_api_key_transport,
    get_rate_limit_seconds,
    get_timeout_seconds,
    get_out_dir,
)
from eth_dataset.http import HttpClient
from eth_dataset.collectors.validators import load_validators_from_args
from eth_dataset.collectors.performance import collect_validator_rows
from eth_dataset.features.trust import compute_trust_v0
from eth_dataset.storage.io import write_outputs

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="ETH-only dataset collector (Beaconcha.in /api/v1)")
    ap.add_argument("--validators", help="Comma-separated validator indexes")
    ap.add_argument("--validators-file", help="File with one validator index per line")
    ap.add_argument("--api-base", default=None, help="Override API base (default: env/API_BASE)")
    ap.add_argument("--api-key", default=None, help="Beaconcha.in API key (env: BEACONCHAIN_API_KEY)")
    ap.add_argument("--key-transport", choices=["header", "query"], default=None,
                    help="Send key in header or query (default: env/API_KEY_TRANSPORT or header)")
    ap.add_argument("--sleep", type=float, default=None, help="Delay between calls (default ~6.2s)")
    ap.add_argument("--timeout", type=int, default=None, help="HTTP timeout seconds (default 30)")
    ap.add_argument("--out-dir", default="eth_dataset/data/ethereum", help="Output dir (default: eth_dataset/data/ethereum)")
    ap.add_argument("--out-prefix", default="validators_mvp", help="Output filename prefix")
    return ap.parse_args()

def main() -> None:
    args = parse_args()

    base = args.api_base or get_api_base()
    key  = args.api_key or get_api_key()
    transport = args.key_transport or get_api_key_transport()

    # Warn if no API key is configured
    if not key:
        print("[WARN] No API key provided; requests may fail or be rejected.", file=sys.stderr)

    sleep = args.sleep if args.sleep is not None else get_rate_limit_seconds()
    timeout = args.timeout if args.timeout is not None else get_timeout_seconds()

    client = HttpClient(
        base_url=base,
        api_key=key,
        api_key_transport=transport,
        rate_limit_seconds=sleep,
        timeout_seconds=timeout,
    )
    # Always use 'eth_dataset/data/ethereum' as output directory
    out_dir = Path(args.out_dir).resolve()

    # Validate and create the directory if it doesn't exist
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[ERROR] Failed to create output directory {out_dir}: {e}", file=sys.stderr)
        sys.exit(1)

    indexes = load_validators_from_args(args.validators, args.validators_file)
    if not indexes:
        print("[ERROR] No validator indexes provided. Use --validators or --validators-file.", file=sys.stderr)
        sys.exit(2)

    rows = collect_validator_rows(client, indexes)
    for r in rows:
        r["trust_v0"] = compute_trust_v0(r)

    write_outputs(rows, out_dir)
    print(f"[OK] Wrote outputs to {out_dir}")

if __name__ == "__main__":
    main()

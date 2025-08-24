from __future__ import annotations
import os
from pathlib import Path
from typing import Optional

# Optional: load .env if python-dotenv is installed
try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
    # Try to locate a .env up the tree; fall back to repo root/".env"
    loaded = load_dotenv(find_dotenv())
    if not loaded:
        repo_root = Path(__file__).resolve().parents[1]  # folder containing eth_dataset/
        load_dotenv(repo_root / ".env")
except Exception:
    pass

API_BASE_DEFAULT = "https://beaconcha.in/api/v1"
RATE_LIMIT_SECONDS_DEFAULT = 6.2   # ~10 req/min free tier
TIMEOUT_SECONDS_DEFAULT = 30
DEFAULT_OUT_DIR = Path("data/eth2/mainnet")

def get_api_base() -> str:
    return os.getenv("API_BASE", API_BASE_DEFAULT)

def get_api_key() -> Optional[str]:
    return os.getenv("BEACONCHAIN_API_KEY")

def get_api_key_transport() -> str:
    """
    'header' -> send as header: apikey: <KEY>
    'query'  -> send as query:  ?apikey=<KEY>
    """
    v = os.getenv("API_KEY_TRANSPORT", "header").strip().lower()
    return v if v in ("header", "query") else "header"

def get_rate_limit_seconds() -> float:
    try:
        return float(os.getenv("RATE_LIMIT_SECONDS", RATE_LIMIT_SECONDS_DEFAULT))
    except Exception:
        return RATE_LIMIT_SECONDS_DEFAULT

def get_timeout_seconds() -> int:
    try:
        return int(os.getenv("HTTP_TIMEOUT_SECONDS", TIMEOUT_SECONDS_DEFAULT))
    except Exception:
        return TIMEOUT_SECONDS_DEFAULT

def get_out_dir(overridden: Optional[str] = None) -> Path:
    p = Path(overridden) if overridden else DEFAULT_OUT_DIR
    p.mkdir(parents=True, exist_ok=True)
    return p

from __future__ import annotations
import os
from pathlib import Path
from typing import Optional

# Optional: load .env if python-dotenv is installed
try:  
    from dotenv import load_dotenv, find_dotenv  # type: ignore  
    # Try common locations without throwing  
    candidates = [  
        Path(find_dotenv(usecwd=True)) if find_dotenv(usecwd=True) else None,  
        Path(__file__).resolve().parent / ".env",          # minimalApproach/code/.env  
        Path(__file__).resolve().parents[1] / ".env",      # minimalApproach/.env  
        Path(__file__).resolve().parents[2] / ".env",      # repo-root/.env (best-effort)  
        Path.cwd() / ".env",                               # current working directory  
    ]  
    for p in [c for c in candidates if c and p is not None]:  
        try:  
            if p.is_file() and load_dotenv(p):  
                break  
        except Exception:  
            continue  
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

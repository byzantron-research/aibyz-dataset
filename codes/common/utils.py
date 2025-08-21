"""Miscellaneous utility functions."""

from datetime import datetime, timezone

def today_str() -> str:
    """Return the current date in ``YYYY‑MM‑DD`` format (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

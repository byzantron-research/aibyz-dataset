"""HTTP helper utilities for retrieving JSON data with retry logic.

This module provides a single function, :func:`get_json`, which wraps the
`requests` library to perform HTTP GET requests with sensible defaults and
automatic retries. The function will always return the parsed JSON payload
on success and will raise an exception if the request ultimately fails.

Key features:

* A shared :class:`requests.Session` instance is used to take advantage of
  connection pooling, which can significantly reduce latency when making
  many requests to the same host.
* A default User‑Agent is set to identify the client when making requests.
* The `tenacity` library is used to implement an exponential backoff
  strategy, retrying failed requests up to a configurable maximum number
  of attempts.

This function is intended to be thread‑safe and is safe to use from
multiple threads. If additional configuration is required (for example,
proxies or custom adapters), you may modify the ``session`` instance
below accordingly.
"""

from typing import Any, Dict, Optional
import requests
import time
import random

__all__ = ["get_json"]

# Shared HTTP session for connection pooling. See
# https://docs.python-requests.org/en/latest/user/advanced/#session-objects
_session = requests.Session()
_DEFAULT_USER_AGENT = "hybrid-dataset/0.1"

_MAX_ATTEMPTS = 5
_INITIAL_DELAY = 0.5  # seconds
_MAX_DELAY = 8.0  # seconds

def get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 20,
) -> Any:
    """Perform a GET request against ``url`` and return the JSON response.

    This function wraps a shared :class:`requests.Session` instance and
    automatically retries on transient errors using an exponential backoff
    strategy with jitter. A default User‑Agent header is always sent and
    can be overridden or supplemented via the ``headers`` parameter.

    :param url: The absolute URL to request.
    :param params: Optional dictionary of query parameters to include.
    :param headers: Optional dictionary of additional headers to send.
    :param timeout: How long to wait (in seconds) for the server to respond.
    :returns: The parsed JSON payload returned by the server.
    :raises: Any exception raised by :mod:`requests` on failure after
        exhausting retries.
    """
    req_headers: Dict[str, str] = {"User-Agent": _DEFAULT_USER_AGENT}
    if headers:
        req_headers.update(headers)
    attempt = 0
    delay = _INITIAL_DELAY
    while True:
        try:
            response = _session.get(
                url,
                params=params or {},
                headers=req_headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except Exception:
            attempt += 1
            if attempt >= _MAX_ATTEMPTS:
                # Re-raise the last exception to the caller
                raise
            # Exponential backoff with jitter
            sleep_time = delay + random.uniform(0, delay)
            time.sleep(sleep_time)
            delay = min(delay * 2, _MAX_DELAY)

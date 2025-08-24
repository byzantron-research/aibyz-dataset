# Copied from http.py to resolve naming conflict
import requests
import threading
import time
from typing import Any, Dict, Optional

class HttpClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_key_transport: str,
        rate_limit_seconds: float,
        timeout_seconds: int,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.api_key_transport = api_key_transport
        self.rate_limit_seconds = rate_limit_seconds
        self.timeout_seconds = timeout_seconds
        self._session = requests.Session()  
        self._lock = threading.Lock()  
        self._next_at = 0.0  
  
    def _wait_for_slot(self) -> None:  
        if self.rate_limit_seconds and self.rate_limit_seconds > 0:  
            with self._lock:  
                now = time.monotonic()  
                if now < self._next_at:  
                    time.sleep(self._next_at - now)  
                self._next_at = max(now, self._next_at) + self.rate_limit_seconds 
    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        headers = {"User-Agent": "aibyz-collector/0.1 (+minimal)", "apikey": self.api_key}
        params = self._inject_key(params, headers)
        url = self._build_url(path)
               
         # Simple pacing  
        self._wait_for_slot()  
        response = self._session.get(url, params=params, headers=headers, timeout=self.timeout_seconds)  
        if response.status_code == 429:  
            # Respect Retry-After if present; else back off using rate_limit_seconds  
            retry_after = response.headers.get("Retry-After")  
            try:  
                delay = float(retry_after) if retry_after is not None else max(self.rate_limit_seconds, 1.0)  
            except Exception:  
                delay = max(self.rate_limit_seconds, 1.0)  
            time.sleep(delay)  
            self._wait_for_slot()  
            response = self._session.get(url, params=params, headers=headers, timeout=self.timeout_seconds)  
        response.raise_for_status()  
        try:  
            return response.json()  
        except ValueError as e:  
            # Surface body for diagnostics without logging secrets  
            raise RuntimeError(f"Non-JSON response from {url}: {response.text[:256]}") from e


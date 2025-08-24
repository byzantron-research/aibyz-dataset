# Copied from http.py to resolve naming conflict
import requests
import threading
import time
from typing import Any, Dict, Optional
import threading
import time

class HttpClient:
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str],
        api_key_transport: str,
        rate_limit_seconds: float,
        timeout_seconds: int,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.api_key_transport = api_key_transport
        self.rate_limit_seconds = rate_limit_seconds
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.last_request_time = 0.0
        self.lock = threading.Lock()

    def _build_url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"
    
    def _inject_key(self, params: Optional[Dict[str, Any]], headers: Dict[str, str]) -> Dict[str, Any]:
        if self.api_key_transport == "header":
            headers["X-API-KEY"] = self.api_key
        elif self.api_key_transport == "query":
            if params is None:
                params = {}
            params["apikey"] = self.api_key
        return params or {}

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        headers = {"User-Agent": "aibyz-collector/0.1 (+minimal)", "apikey": self.api_key}
        params = self._inject_key(params, headers)
        url = self._build_url(path)

        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_request_time
            if elapsed < self.rate_limit_seconds:
                time.sleep(self.rate_limit_seconds - elapsed)
            self.last_request_time = time.monotonic()

        response = self.session.get(url, params=params, headers=headers, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()

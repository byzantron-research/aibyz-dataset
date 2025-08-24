# Copied from http.py to resolve naming conflict
import requests
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

    def _build_url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _inject_key(self, params: Optional[Dict[str, Any]], headers: Dict[str, str]) -> Dict[str, Any]:
        if self.api_key_transport != "query":
            raise ValueError("Only 'query' transport is supported")

        if params is None:
            params = {}
        params["apikey"] = self.api_key

        return params

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        headers = {}
        params = self._inject_key(params, headers)
        url = self._build_url(path)
        response = requests.get(url, params=params, headers=headers, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()

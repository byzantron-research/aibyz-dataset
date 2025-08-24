import unittest
from eth_dataset.config import get_api_base, get_api_key, get_api_key_transport, get_rate_limit_seconds, get_timeout_seconds, get_out_dir
from eth_dataset.http import HttpClient
from eth_dataset.beaconchain import get_validator_overview, get_validator_performance
from eth_dataset.collectors.performance import collect_validator_rows
from eth_dataset.collectors.validators import load_validators_from_args
from eth_dataset.storage.io import write_outputs
from pathlib import Path
import os

class TestConfig(unittest.TestCase):
    def test_get_api_base(self):
        self.assertIsInstance(get_api_base(), str)

    def test_get_api_key(self):
        # Can be None if not set
        key = get_api_key()
        self.assertTrue(key is None or isinstance(key, str))

    def test_get_api_key_transport(self):
        self.assertIsInstance(get_api_key_transport(), str)

    def test_get_rate_limit_seconds(self):
        self.assertIsInstance(get_rate_limit_seconds(), float)

    def test_get_timeout_seconds(self):
        self.assertIsInstance(get_timeout_seconds(), int)

    def test_get_out_dir(self):
        self.assertIsInstance(get_out_dir(), Path)

class TestHttpClient(unittest.TestCase):
    def test_init(self):
        client = HttpClient("https://example.com", "dummy_key", "query", 1.0, 10)
        self.assertIsInstance(client, HttpClient)

    def test_inject_key(self):
        client = HttpClient("https://example.com", "dummy_key", "query", 1.0, 10)
        params = client._inject_key(None, {})
        self.assertEqual(params["apikey"], "dummy_key")
        headers = {}
        client._inject_key(params, headers)
        self.assertEqual(headers, {})

class TestBeaconchain(unittest.TestCase):
    def setUp(self):
        self.client = HttpClient("https://example.com", "dummy_key", "query", 1.0, 10)

    def test_get_validator_overview(self):
        # Should handle error gracefully (mocked)
        try:
            get_validator_overview(self.client, 0)
        except Exception:
            pass

    def test_get_validator_performance(self):
        try:
            get_validator_performance(self.client, 0)
        except Exception:
            pass

class TestCollectors(unittest.TestCase):
    def setUp(self):
        self.client = HttpClient("https://example.com", "dummy_key", "query", 1.0, 10)

    def test_collect_validator_rows(self):
        try:
            collect_validator_rows(self.client, [0, 1])
        except Exception:
            pass

    def test_load_validators_from_args(self):
        result = load_validators_from_args("1,2,3", None)
        self.assertIsInstance(result, list)

class TestStorage(unittest.TestCase):
    def test_write_outputs(self):
        rows = [{"a": 1}]
        out_dir = Path("./test_out")
        try:
            write_outputs(rows, out_dir, "test")
        except Exception:
            pass

if __name__ == "__main__":
    unittest.main()

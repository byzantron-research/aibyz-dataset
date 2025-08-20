"""Cosmos SDK data collector.

This module defines :class:`CosmosCollector`, responsible for retrieving
blocks, validators, attestations (derived from Tendermint commits) and
penalties (via the slashing signing info) from a Cosmos SDK LCD
(gRPC‑gateway) endpoint. Concurrency is supported for block and
attestation ingestion via the ``max_workers`` configuration parameter.
"""

from __future__ import annotations

from typing import List, Optional, Dict, Any
from pathlib import Path
from tqdm import tqdm
import json as _json
from datetime import datetime, timezone
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.http import get_json
from common.storage import write_rows, part_path, write_provenance
from common.provenance import Provenance
from common.schemas import Block, Validator, Attestation, Penalty

logger = logging.getLogger(__name__)

# Cosmos SDK LCD (gRPC-gateway) endpoints:
# - Blocks:  /cosmos/base/tendermint/v1beta1/blocks/{height}
# - Latest:  /cosmos/base/tendermint/v1beta1/blocks/latest
# - Validators: /cosmos/staking/v1beta1/validators?status=BOND_STATUS_BONDED&pagination.limit=...
# - Signing infos: /cosmos/slashing/v1beta1/signing_infos (proxy for "penalty-like" rows)
# See Cosmos SDK gRPC-gateway docs. :contentReference[oaicite:4]{index=4}

class CosmosCollector:
    """Collects data from a Cosmos SDK chain via its LCD/gRPC gateway.

    :param cfg: Configuration dictionary. Recognised keys include:
        * ``network``: Network name (default ``"cosmoshub"``).
        * ``lcd``: Base URL of the LCD/gRPC REST gateway.
        * ``root``: Root directory for output data.
        * ``format``: Output format (``"parquet"`` or ``"csv"``).
        * ``headers``: Optional HTTP headers.
        * ``max_workers``: Optional concurrency level for fetching blocks and
          commits. Defaults to 1 (sequential).
    """

    def __init__(self, cfg: dict) -> None:
        self.chain_id: str = "cosmos"
        self.network: str = cfg.get("network", "cosmoshub")
        self.base: str = cfg.get("lcd", "http://localhost:1317").rstrip("/")
        self.format: str = cfg.get("format", "parquet")
        self.root: Path = Path(cfg.get("root", "data"))
        self.headers: Dict[str, str] = cfg.get("headers", {})
        self.max_workers: int = int(cfg.get("max_workers", 1))

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Perform a GET request against the LCD API and return the JSON payload."""
        url = self.base + path
        return get_json(url, params=params or {}, headers=self.headers)

    def _head_height(self) -> int:
        """Return the current chain height as reported by the LCD endpoint."""
        data = self._get("/cosmos/base/tendermint/v1beta1/blocks/latest")
        return int(data["block"]["header"]["height"])

    def collect(
        self,
        datasets: List[str],
        ingest_date: str,
        start_height: Optional[int] = None,
        end_height: Optional[int] = None,
        lookback_blocks: Optional[int] = None,
    ) -> None:
        """Collect one or more datasets for a range of block heights.

        If ``start_height`` or ``end_height`` are omitted, a lookback window
        controlled by ``lookback_blocks`` is used. At least one block will
        always be collected.

        :param datasets: Dataset identifiers (``"blocks"``, ``"validators"``,
          ``"attestations"``, ``"penalties"``).
        :param ingest_date: Date string used for partitioning output directories.
        :param start_height: Starting block height, inclusive.
        :param end_height: Ending block height, inclusive.
        :param lookback_blocks: Number of blocks to look back from the head
          when ``start_height`` or ``end_height`` are missing. Defaults to 2000.
        """
        if start_height is None or end_height is None:
            head = self._head_height()
            lb = int(lookback_blocks or 2000)
            start, end = max(1, head - lb + 1), head
        else:
            start, end = int(start_height), int(end_height)
        if end < start:
            raise ValueError(f"Invalid height range: start={start} end={end}")
        if "blocks" in datasets:
            self._blocks(start, end, ingest_date)
        if "validators" in datasets:
            self._validators(ingest_date)
        if "attestations" in datasets:
            self._attestations_from_commits(start, end, ingest_date)
        if "penalties" in datasets:
            self._signing_infos(ingest_date)

    def _blocks(self, start: int, end: int, date: str) -> None:
        """Collect block headers for a range of heights."""
        def fetch_block(height: int) -> Optional[Dict[str, Any]]:
            try:
                b = self._get(f"/cosmos/base/tendermint/v1beta1/blocks/{height}")
                hdr = b["block"]["header"]
                return Block(
                    chain_id=self.chain_id,
                    network=self.network,
                    height_or_slot=int(height),
                    epoch=None,
                    block_hash=b.get("block_id", {}).get("hash"),
                    parent_hash=None,
                    proposer_index=None,
                    proposer_address=hdr.get("proposer_address"),
                    timestamp_utc=int(
                        datetime.fromisoformat(hdr["time"].replace("Z", "+00:00")).timestamp()
                    ),
                ).model_dump()
            except Exception as e:
                logger.exception("cosmos._blocks failed for height %s: %s", height, e)
                return None
        total = end - start + 1
        rows: List[Dict[str, Any]] = []
        if self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(fetch_block, h): h for h in range(start, end + 1)}
                with tqdm(total=total, desc=f"{self.network} blocks", unit="block") as pbar:
                    for fut in as_completed(futures):
                        res = fut.result()
                        if res is not None:
                            rows.append(res)
                        pbar.update(1)
        else:
            for h in tqdm(range(start, end + 1), desc=f"{self.network} blocks", unit="block"):
                res = fetch_block(h)
                if res is not None:
                    rows.append(res)
        out = part_path(self.root, "raw", "blocks", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v1beta1",
            collector="cosmos.blocks",
            chain_id=self.chain_id,
            network=self.network,
            dataset="blocks",
            rows=len(rows),
        ).to_dict())

    def _validators(self, date: str) -> None:
        """Collect the current set of active (bonded) validators."""
        rows: List[Dict[str, Any]] = []
        now = int(datetime.now(timezone.utc).timestamp())
        page_key: Optional[str] = None
        while True:
            params = {"status": "BOND_STATUS_BONDED", "pagination.limit": "200"}
            if page_key:
                params["pagination.key"] = page_key
            try:
                data = self._get("/cosmos/staking/v1beta1/validators", params=params)
            except Exception as e:
                logger.exception("cosmos._validators fetch failed: %s", e)
                break
            for v in data.get("validators", []) or []:
                try:
                    rows.append(
                        Validator(
                            chain_id=self.chain_id,
                            network=self.network,
                            snapshot_ts=now,
                            validator_id=v.get("operator_address"),
                            status="BONDED",
                            balance=None,
                            effective_balance=None,
                            pubkey=(v.get("consensus_pubkey") or {}).get("key"),
                        ).model_dump()
                    )
                except Exception as e:
                    logger.exception("cosmos._validators row parse failed: %s", e)
                    continue
            page_key = (data.get("pagination") or {}).get("next_key")
            if not page_key:
                break
        out = part_path(self.root, "raw", "validators", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v1beta1",
            collector="cosmos.validators",
            chain_id=self.chain_id,
            network=self.network,
            dataset="validators",
            rows=len(rows),
        ).to_dict())

    def _attestations_from_commits(self, start: int, end: int, date: str) -> None:
        """Map Tendermint commit signatures to attestation‑like records."""

        def fetch_commit(height: int) -> Optional[List[Dict[str, Any]]]:
            try:
                b = self._get(f"/cosmos/base/tendermint/v1beta1/blocks/{height}")
                commit = b.get("block", {}).get("last_commit", {}) or {}
                rows_for_height: List[Dict[str, Any]] = []
                for _sig in commit.get("signatures", []) or []:
                    try:
                        rows_for_height.append(
                            Attestation(
                                chain_id=self.chain_id,
                                network=self.network,
                                height_or_slot=int(height),
                                epoch=None,
                                committee_index=None,
                                head_block_root=(b.get("block_id") or {}).get("hash"),
                                source_epoch=None,
                                target_epoch=None,
                            ).model_dump()
                        )
                    except Exception as e:
                        logger.exception(
                            "cosmos._attestations_from_commits row parse failed: %s", e
                        )
                        continue
                return rows_for_height
            except Exception as e:
                logger.exception(
                    "cosmos._attestations_from_commits failed for height %s: %s", height, e
                )
                return None
        rows: List[Dict[str, Any]] = []
        total = end - start + 1
        if self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(fetch_commit, h): h for h in range(start, end + 1)
                }
                with tqdm(
                    total=total, desc=f"{self.network} commits", unit="block"
                ) as pbar:
                    for fut in as_completed(futures):
                        res = fut.result()
                        if res:
                            rows.extend(res)
                        pbar.update(1)
        else:
            for h in tqdm(range(start, end + 1), desc=f"{self.network} commits", unit="block"):
                res = fetch_commit(h)
                if res:
                    rows.extend(res)
        out = part_path(self.root, "raw", "attestations", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v1beta1",
            collector="cosmos.attestations",
            chain_id=self.chain_id,
            network=self.network,
            dataset="attestations",
            rows=len(rows),
        ).to_dict())

    def _signing_infos(self, date: str) -> None:
        """Collect signing info (slashing) events as penalty records."""
        rows: List[Dict[str, Any]] = []
        page_key: Optional[str] = None
        while True:
            params = {"pagination.limit": "200"}
            if page_key:
                params["pagination.key"] = page_key
            try:
                data = self._get("/cosmos/slashing/v1beta1/signing_infos", params=params)
            except Exception as e:
                logger.exception("cosmos._signing_infos fetch failed: %s", e)
                break
            for si in data.get("info", []) or []:
                try:
                    rows.append(
                        Penalty(
                            chain_id=self.chain_id,
                            network=self.network,
                            height_or_slot=0,  # snapshot item
                            validator_id=si.get("address"),
                            penalty_type="signing_info",
                            value=None,
                            meta_json=_json.dumps(si),
                        ).model_dump()
                    )
                except Exception as e:
                    logger.exception("cosmos._signing_infos row parse failed: %s", e)
                    continue
            page_key = (data.get("pagination") or {}).get("next_key")
            if not page_key:
                break
        out = part_path(self.root, "raw", "penalties", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v1beta1",
            collector="cosmos.penalties",
            chain_id=self.chain_id,
            network=self.network,
            dataset="penalties",
            rows=len(rows),
        ).to_dict())

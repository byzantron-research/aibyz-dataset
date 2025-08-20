"""Ethereum 2 (Beacon chain) data collector.

This module defines :class:`Eth2Collector`, responsible for retrieving
blocks, validators, attestations and penalties from an Ethereum 2 beacon
node's REST API. The collector can operate in either sequential or
concurrent mode: concurrency is controlled via the ``max_workers``
configuration option. When enabled, blocks and attestations are fetched
in parallel using a :class:`concurrent.futures.ThreadPoolExecutor`,
significantly speeding up ingestion when large windows are requested.

All retrieved data is immediately written to disk in partitioned
directories via the helpers in :mod:`common.storage`. A provenance file
is also emitted alongside each dataset, capturing metadata about the
collection event (e.g. source base URL, API version).
"""

from __future__ import annotations

from typing import List, Optional, Dict, Any, Iterable
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

class Eth2Collector:
    """Collects Ethereum 2 (Beacon chain) data from a REST API.

    :param cfg: Configuration dictionary. Recognised keys include:
        * ``network``: Network name (default ``"mainnet"``).
        * ``beacon``: Base URL of the Beacon REST API.
        * ``root``: Root directory for output data.
        * ``format``: Output format (``"parquet"`` or ``"csv"``).
        * ``headers``: Optional HTTP headers (e.g. API keys).
        * ``max_workers``: Optional integer controlling concurrency. Values
          greater than one will enable concurrent fetching of slots for
          blocks, attestations and penalties. Defaults to 1 (sequential).
    """

    def __init__(self, cfg: dict) -> None:
        # Basic chain metadata
        self.chain_id: str = "eth2"
        self.network: str = cfg.get("network", "mainnet")
        # Base URL for the Beacon REST API, stripped of any trailing slash
        self.base: str = cfg.get("beacon", "http://localhost:5052").rstrip("/")
        # Output configuration
        self.format: str = cfg.get("format", "parquet")
        self.root: Path = Path(cfg.get("root", "data"))
        # Optional headers for API authentication
        self.headers: Dict[str, str] = cfg.get("headers", {})
        # Concurrency control: number of worker threads
        self.max_workers: int = int(cfg.get("max_workers", 1))

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Perform a GET request against the Beacon API and return the JSON payload."""
        url = self.base + path
        return get_json(url, params=params or {}, headers=self.headers)

    def _head_slot(self) -> int:
        """Retrieve the latest known slot from the beacon node."""
        data = self._get("/eth/v1/beacon/headers")
        return int(data["data"][0]["header"]["message"]["slot"])

    def collect(
        self,
        datasets: List[str],
        ingest_date: str,
        start_slot: Optional[int] = None,
        end_slot: Optional[int] = None,
        lookback_slots: Optional[int] = None,
    ) -> None:
        """Collect one or more datasets for the specified slot range.

        When ``start_slot`` or ``end_slot`` are not provided, a lookback window
        is derived from the current head slot and ``lookback_slots``. At least
        one slot will always be collected.

        :param datasets: List of dataset identifiers to collect. Valid
          values include ``"blocks"``, ``"validators"``, ``"attestations"``
          and ``"penalties"``.
        :param ingest_date: Date string (``YYYY‑MM‑DD``) used for partitioning
          the output directories.
        :param start_slot: Starting slot, inclusive. When provided,
          ``end_slot`` must also be supplied.
        :param end_slot: Ending slot, inclusive.
        :param lookback_slots: Number of slots to look back from the head
          if ``start_slot`` and ``end_slot`` are omitted. Defaults to 512.
        """
        if start_slot is None or end_slot is None:
            head = self._head_slot()
            lb = int(lookback_slots or 512)
            start, end = max(0, head - lb + 1), head
        else:
            start, end = int(start_slot), int(end_slot)
        # Ensure there is at least one slot to process
        if end < start:
            raise ValueError(f"Invalid slot range: start={start} end={end}")
        # Dispatch to dataset-specific collectors
        if "blocks" in datasets:
            self._blocks(start, end, ingest_date)
        if "validators" in datasets:
            self._validators(ingest_date)
        if "attestations" in datasets:
            self._attestations(start, end, ingest_date)
        if "penalties" in datasets:
            self._penalties(start, end, ingest_date)

    def _blocks(self, start: int, end: int, date: str) -> None:
        """Collect block headers for a range of slots and persist them to disk."""
        def fetch_block(slot: int) -> Optional[Dict[str, Any]]:
            """Helper to fetch and parse a single block. Returns None on error."""
            try:
                b = self._get(f"/eth/v2/beacon/blocks/{slot}")["data"]["message"]
                return Block(
                    chain_id=self.chain_id,
                    network=self.network,
                    height_or_slot=int(b["slot"]),
                    epoch=int(b["slot"]) // 32,
                    block_hash=None,
                    parent_hash=None,
                    proposer_index=int(b.get("proposer_index", -1)),
                    timestamp_utc=None,
                ).model_dump()
            except Exception as e:
                logger.exception("eth2._blocks failed for slot %s: %s", slot, e)
                return None

        rows: List[Dict[str, Any]] = []
        total = end - start + 1
        if self.max_workers > 1:
            # Concurrent execution
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(fetch_block, slot): slot for slot in range(start, end + 1)}
                with tqdm(total=total, desc="eth2 blocks", unit="slot") as pbar:
                    for future in as_completed(futures):
                        res = future.result()
                        if res is not None:
                            rows.append(res)
                        pbar.update(1)
        else:
            # Sequential execution
            for slot in tqdm(range(start, end + 1), desc="eth2 blocks", unit="slot"):
                row = fetch_block(slot)
                if row is not None:
                    rows.append(row)
        # Write out rows and provenance
        out = part_path(self.root, "raw", "blocks", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v2",
            collector="eth2.blocks",
            chain_id=self.chain_id,
            network=self.network,
            dataset="blocks",
            rows=len(rows),
        ).to_dict())

    def _validators(self, date: str) -> None:
        """Collect a snapshot of all validators at the current head state."""
        rows: List[Dict[str, Any]] = []
        try:
            data = self._get("/eth/v1/beacon/states/head/validators")
        except Exception as e:
            logger.exception("eth2._validators fetch failed: %s", e)
            # Even if the fetch fails we still write an empty file to mark the attempt
            data = {}
        now = int(datetime.now(timezone.utc).timestamp())
        for v in data.get("data", []) or []:
            try:
                info = v.get("validator", {}) or {}
                rows.append(
                    Validator(
                        chain_id=self.chain_id,
                        network=self.network,
                        snapshot_ts=now,
                        validator_id=str(v.get("index")),
                        status=str(v.get("status")),
                        balance=float(info.get("balance", 0)) / 1e9 if info.get("balance") is not None else None,
                        effective_balance=float(info.get("effective_balance", 0)) / 1e9 if info.get("effective_balance") is not None else None,
                        pubkey=info.get("pubkey"),
                    ).model_dump()
                )
            except Exception as e:
                logger.exception("eth2._validators row parse failed: %s", e)
                continue
        out = part_path(self.root, "raw", "validators", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v1",
            collector="eth2.validators",
            chain_id=self.chain_id,
            network=self.network,
            dataset="validators",
            rows=len(rows),
        ).to_dict())

    def _attestations(self, start: int, end: int, date: str) -> None:
        """Collect attestations from blocks in a slot range."""

        def fetch_att(slot: int) -> Optional[List[Dict[str, Any]]]:
            """Fetch attestations for a single slot. Returns a list of row dicts."""
            try:
                b = self._get(f"/eth/v2/beacon/blocks/{slot}")["data"]["message"]
                rows_for_slot: List[Dict[str, Any]] = []
                for att in b["body"].get("attestations", []) or []:
                    ad = att.get("data", {}) or {}
                    rows_for_slot.append(
                        Attestation(
                            chain_id=self.chain_id,
                            network=self.network,
                            height_or_slot=int(b["slot"]),
                            epoch=int(b["slot"]) // 32,
                            committee_index=int(ad.get("index")) if ad.get("index") is not None else None,
                            head_block_root=ad.get("beacon_block_root"),
                            source_epoch=int(ad.get("source", {}).get("epoch")) if ad.get("source") else None,
                            target_epoch=int(ad.get("target", {}).get("epoch")) if ad.get("target") else None,
                        ).model_dump()
                    )
                return rows_for_slot
            except Exception as e:
                logger.exception("eth2._attestations failed for slot %s: %s", slot, e)
                return None

        all_rows: List[Dict[str, Any]] = []
        total = end - start + 1
        if self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(fetch_att, slot): slot for slot in range(start, end + 1)}
                with tqdm(total=total, desc="eth2 attestations", unit="slot") as pbar:
                    for fut in as_completed(futures):
                        res = fut.result()
                        if res:
                            all_rows.extend(res)
                        pbar.update(1)
        else:
            for slot in tqdm(range(start, end + 1), desc="eth2 attestations", unit="slot"):
                res = fetch_att(slot)
                if res:
                    all_rows.extend(res)
        out = part_path(self.root, "raw", "attestations", self.chain_id, self.network, date)
        write_rows(all_rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v2",
            collector="eth2.attestations",
            chain_id=self.chain_id,
            network=self.network,
            dataset="attestations",
            rows=len(all_rows),
        ).to_dict())

    def _penalties(self, start: int, end: int, date: str) -> None:
        """Collect slashing events (penalties) from blocks in a slot range."""

        def fetch_penalties(slot: int) -> Optional[List[Dict[str, Any]]]:
            try:
                b = self._get(f"/eth/v2/beacon/blocks/{slot}")["data"]["message"]
                body = b["body"]
                rows_for_slot: List[Dict[str, Any]] = []
                for s in body.get("proposer_slashings", []) or []:
                    rows_for_slot.append(
                        Penalty(
                            chain_id=self.chain_id,
                            network=self.network,
                            height_or_slot=int(b["slot"]),
                            validator_id=None,
                            penalty_type="proposer_slashing",
                            value=None,
                            meta_json=_json.dumps(s),
                        ).model_dump()
                    )
                for s in body.get("attester_slashings", []) or []:
                    rows_for_slot.append(
                        Penalty(
                            chain_id=self.chain_id,
                            network=self.network,
                            height_or_slot=int(b["slot"]),
                            validator_id=None,
                            penalty_type="attester_slashing",
                            value=None,
                            meta_json=_json.dumps(s),
                        ).model_dump()
                    )
                return rows_for_slot
            except Exception as e:
                logger.exception("eth2._penalties failed for slot %s: %s", slot, e)
                return None
        all_rows: List[Dict[str, Any]] = []
        total = end - start + 1
        if self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(fetch_penalties, slot): slot for slot in range(start, end + 1)}
                with tqdm(total=total, desc="eth2 penalties", unit="slot") as pbar:
                    for fut in as_completed(futures):
                        res = fut.result()
                        if res:
                            all_rows.extend(res)
                        pbar.update(1)
        else:
            for slot in tqdm(range(start, end + 1), desc="eth2 penalties", unit="slot"):
                res = fetch_penalties(slot)
                if res:
                    all_rows.extend(res)
        out = part_path(self.root, "raw", "penalties", self.chain_id, self.network, date)
        write_rows(all_rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.base,
            api_version="v2",
            collector="eth2.penalties",
            chain_id=self.chain_id,
            network=self.network,
            dataset="penalties",
            rows=len(all_rows),
        ).to_dict())

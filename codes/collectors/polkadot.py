"""Substrate/Polkadot data collector.

This module defines :class:`PolkadotCollector`, which uses the
`py‑substrate‑interface` library to interact with a Substrate‑based chain
(e.g. Polkadot or Kusama). It retrieves block headers, the current
validator set, and slashing events in a specified height range.

At present this collector operates sequentially. Concurrency is not
implemented because the underlying WebSocket interface provided by
`py‑substrate‑interface` does not lend itself to safe concurrent use.
"""

from __future__ import annotations

from typing import List, Optional
from pathlib import Path
from tqdm import tqdm
import json as _json
from datetime import datetime, timezone
import logging

try:
    # The substrateinterface package is optional and may not be installed in all
    # environments. Import it lazily to allow the module to be imported without
    # immediately raising if the dependency is missing. See __init__ for details.
    from substrateinterface import SubstrateInterface  # type: ignore
except Exception:
    SubstrateInterface = None  # type: ignore
from common.storage import write_rows, part_path, write_provenance
from common.provenance import Provenance
from common.schemas import Block, Validator, Penalty

logger = logging.getLogger(__name__)

# Uses py-substrate-interface to query a Substrate/Polkadot node (recommended in official docs).
# We retrieve:
#  - blocks: headers via get_block for a range
#  - validators: Session.Validators at the time of query (snapshot)
#  - penalties: scan block events for Staking Slash/Slashed for the window
# :contentReference[oaicite:5]{index=5}

class PolkadotCollector:
    """Collects block, validator and penalty data from a Substrate node.

    :param cfg: Configuration dictionary. Recognised keys include:
        * ``network``: Network name (default ``"mainnet"``).
        * ``rpc``: WebSocket RPC endpoint URL.
        * ``root``: Root directory for output data.
        * ``format``: Output format (``"parquet"`` or ``"csv"``).
    """

    def __init__(self, cfg: dict) -> None:
        self.chain_id: str = "polkadot"
        self.network: str = cfg.get("network", "mainnet")
        self.rpc: str = cfg.get("rpc", "wss://rpc.polkadot.io")
        self.format: str = cfg.get("format", "parquet")
        self.root: Path = Path(cfg.get("root", "data"))
        # Defer importing SubstrateInterface until instantiation time. If the
        # optional dependency is missing, raise an informative error.
        if SubstrateInterface is None:
            raise ImportError(
                "py-substrate-interface is required for PolkadotCollector. "
                "Install it via pip to enable Polkadot data collection."
            )
        # Initialise a substrate interface. When connecting to a private node
        # you may need to specify additional parameters (e.g. type registry).
        self.substrate = SubstrateInterface(url=self.rpc)

    def _head_number(self) -> int:
        """Return the current best block number reported by the node."""
        hdr = self.substrate.get_chain_head()
        header = self.substrate.get_block_header(hdr)
        return int(header["number"])

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

        :param datasets: Dataset identifiers (``"blocks"``, ``"validators"``, ``"penalties"``).
        :param ingest_date: Date string used for partitioning output directories.
        :param start_height: Starting block height, inclusive.
        :param end_height: Ending block height, inclusive.
        :param lookback_blocks: Number of blocks to look back when start/end are missing.
        """
        if start_height is None or end_height is None:
            head = self._head_number()
            lb = int(lookback_blocks or 1024)
            start, end = max(1, head - lb + 1), head
        else:
            start, end = int(start_height), int(end_height)
        if end < start:
            raise ValueError(f"Invalid height range: start={start} end={end}")
        if "blocks" in datasets:
            self._blocks(start, end, ingest_date)
        if "validators" in datasets:
            self._validators(ingest_date)
        if "penalties" in datasets:
            self._penalties(start, end, ingest_date)
        # (No "attestations" concept for Substrate/Polkadot; skip)

    def _blocks(self, start: int, end: int, date: str) -> None:
        """Collect block headers in a sequential manner for a height range."""
        rows: List[dict] = []
        for h in tqdm(range(start, end + 1), desc="polkadot blocks", unit="block"):
            try:
                block_hash = self.substrate.get_block_hash(h)
                if block_hash is None:
                    continue
                block = self.substrate.get_block(block_hash=block_hash)
                header = block["header"]
                ts: Optional[int] = None
                for ex in block.get("extrinsics", []) or []:
                    if (
                        ex["call"]["call_module"] == "Timestamp"
                        and ex["call"]["call_function"] == "set"
                    ):
                        # first arg is moment (milliseconds)
                        ts = int(ex["params"][0]["value"]) // 1000
                        break
                rows.append(
                    Block(
                        chain_id=self.chain_id,
                        network=self.network,
                        height_or_slot=int(h),
                        epoch=None,
                        block_hash=str(block_hash),
                        parent_hash=str(header["parentHash"]),
                        proposer_index=None,
                        proposer_address=None,
                        timestamp_utc=ts,
                    ).model_dump()
                )
            except Exception as e:
                logger.exception("polkadot._blocks failed for height %s: %s", h, e)
                continue
        out = part_path(self.root, "raw", "blocks", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.rpc,
            api_version="rpc",
            collector="polkadot.blocks",
            chain_id=self.chain_id,
            network=self.network,
            dataset="blocks",
            rows=len(rows),
        ).to_dict())

    def _validators(self, date: str) -> None:
        """Collect the current validator set snapshot."""
        rows: List[dict] = []
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            vals = self.substrate.query(module="Session", storage_function="Validators").value
            for vid in vals or []:
                rows.append(
                    Validator(
                        chain_id=self.chain_id,
                        network=self.network,
                        snapshot_ts=now,
                        validator_id=str(vid),
                        status="ACTIVE",
                        balance=None,
                        effective_balance=None,
                        pubkey=None,
                    ).model_dump()
                )
        except Exception as e:
            logger.exception("polkadot._validators fetch failed: %s", e)
        out = part_path(self.root, "raw", "validators", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.rpc,
            api_version="rpc",
            collector="polkadot.validators",
            chain_id=self.chain_id,
            network=self.network,
            dataset="validators",
            rows=len(rows),
        ).to_dict())

    def _penalties(self, start: int, end: int, date: str) -> None:
        """Collect staking slashing events in a height range."""
        rows: List[dict] = []
        for h in tqdm(range(start, end + 1), desc="polkadot penalties", unit="block"):
            try:
                block_hash = self.substrate.get_block_hash(h)
                if block_hash is None:
                    continue
                events = self.substrate.get_events(block_hash=block_hash)
                for ev in events:
                    if (
                        ev.value["module_id"] == "Staking"
                        and ev.value["event_id"] in ("Slash", "Slashed")
                    ):
                        # The event attributes vary; capture the raw event JSON
                        rows.append(
                            Penalty(
                                chain_id=self.chain_id,
                                network=self.network,
                                height_or_slot=int(h),
                                validator_id=None,
                                penalty_type="staking_slash",
                                value=None,
                                meta_json=_json.dumps(ev.value),
                            ).model_dump()
                        )
            except Exception as e:
                logger.exception("polkadot._penalties failed for height %s: %s", h, e)
                continue
        out = part_path(self.root, "raw", "penalties", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(
            source=self.rpc,
            api_version="rpc",
            collector="polkadot.penalties",
            chain_id=self.chain_id,
            network=self.network,
            dataset="penalties",
            rows=len(rows),
        ).to_dict())

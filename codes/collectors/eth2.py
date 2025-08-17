from typing import List, Optional, Dict, Any
from pathlib import Path
from tqdm import tqdm
import json as _json
from common.http import get_json
from common.storage import write_rows, part_path, write_provenance
from common.provenance import Provenance
from common.schemas import Block, Validator, Attestation, Penalty

class Eth2Collector:
    def __init__(self, cfg: dict):
        self.chain_id = "eth2"
        self.network = cfg.get("network", "mainnet")
        self.base = cfg.get("beacon", "http://localhost:5052")
        self.format = cfg.get("format", "parquet")
        self.root = Path(cfg.get("root", "."))

    def _get(self, path: str, params: Dict[str, Any] = None):
        return get_json(f"{self.base}{path}", params=params or {})

    def _latest_slot(self) -> int:
        # Head slot via beacon headers
        res = self._get("/eth/v1/beacon/headers")
        return int(res["data"][0]["header"]["message"]["slot"])

    def collect(self, datasets: List[str], start: Optional[int], end: Optional[int], limit: Optional[int], ingest_date: str):
        # Windowing defaults
        if start is None and end is None:
            latest = self._latest_slot()
            end = latest
            lookback = limit or 512
            start = max(0, latest - lookback + 1)
        elif end is None:
            latest = self._latest_slot()
            end = min(latest, start + (limit or 256) - 1)

        if "blocks" in datasets:
            self._blocks(start, end, ingest_date)
        if "validators" in datasets:
            self._validators(ingest_date)
        if "attestations" in datasets:
            self._attestations(start, end, ingest_date)
        if "penalties" in datasets:
            self._penalties(start, end, ingest_date)

    def _blocks(self, start: int, end: int, date: str):
        rows = []
        for slot in tqdm(range(start, end + 1), desc="eth2 blocks"):
            try:
                b = self._get(f"/eth/v2/beacon/blocks/{slot}")
                msg = b["data"]["message"]
                rows.append(Block(
                    chain_id=self.chain_id, network=self.network,
                    height_or_slot=int(msg["slot"]),
                    epoch=int(msg["slot"]) // 32,
                    block_hash=b["data"]["root"],
                    parent_hash=msg["parent_root"],
                    proposer_index=int(msg["proposer_index"]),
                    timestamp_utc=None
                ).model_dump())
            except Exception:
                continue
        out = part_path(self.root, "raw", "blocks", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(self.base, None, "eth2.blocks", self.chain_id, self.network, "blocks", len(rows)).to_dict())

    def _validators(self, date: str):
        rows = []
        # Use "head" state for snapshot
        res = self._get("/eth/v1/beacon/states/head/validators", params={"status": "active"})
        for v in res["data"]:
            info = v["validator"]
            rows.append(Validator(
                chain_id=self.chain_id, network=self.network,
                snapshot_ts=0,
                validator_id=str(v["index"]),
                status=v.get("status"),
                balance=int(info.get("balance", 0)),
                effective_balance=int(info.get("effective_balance", 0)),
                slashed=bool(info.get("slashed", False)),
                withdrawal_address=None
            ).model_dump())
        out = part_path(self.root, "raw", "validators", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(self.base, None, "eth2.validators", self.chain_id, self.network, "validators", len(rows)).to_dict())

    def _attestations(self, start: int, end: int, date: str):
        rows = []
        for slot in tqdm(range(start, end + 1), desc="eth2 attestations"):
            try:
                b = self._get(f"/eth/v2/beacon/blocks/{slot}")["data"]["message"]
                for att in b["body"].get("attestations", []):
                    d = att["data"]
                    rows.append(Attestation(
                        chain_id=self.chain_id, network=self.network,
                        height_or_slot=int(b["slot"]),
                        epoch=int(b["slot"]) // 32,
                        committee_index=int(d.get("index", 0)),
                        head_block_root=d.get("beacon_block_root"),
                        source_epoch=int(d.get("source", {}).get("epoch", 0)),
                        target_epoch=int(d.get("target", {}).get("epoch", 0)),
                    ).model_dump())
            except Exception:
                continue
        out = part_path(self.root, "raw", "attestations", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(self.base, None, "eth2.attestations", self.chain_id, self.network, "attestations", len(rows)).to_dict())

    def _penalties(self, start: int, end: int, date: str):
        rows = []
        for slot in tqdm(range(start, end + 1), desc="eth2 penalties"):
            try:
                b = self._get(f"/eth/v2/beacon/blocks/{slot}")["data"]["message"]
                body = b["body"]
                for s in body.get("proposer_slashings", []):
                    rows.append(Penalty(chain_id=self.chain_id, network=self.network,
                        height_or_slot=int(b["slot"]), penalty_type="proposer_slashing",
                        meta_json=_json.dumps(s)).model_dump())
                for s in body.get("attester_slashings", []):
                    rows.append(Penalty(chain_id=self.chain_id, network=self.network,
                        height_or_slot=int(b["slot"]), penalty_type="attester_slashing",
                        meta_json=_json.dumps(s)).model_dump())
            except Exception:
                continue
        out = part_path(self.root, "raw", "penalties", self.chain_id, self.network, date)
        write_rows(rows, out, self.format)
        write_provenance(out, Provenance(self.base, None, "eth2.penalties", self.chain_id, self.network, "penalties", len(rows)).to_dict())
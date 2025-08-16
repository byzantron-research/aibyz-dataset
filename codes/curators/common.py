from pathlib import Path
import pandas as pd
from common.storage import ensure_dir, part_path, write_rows

class Curator:
    def __init__(self, cfg: dict):
        self.chain_id = cfg["chain_id"]
        self.network = cfg["network"]
        self.root = Path(cfg.get("root", "."))
        self.format = cfg.get("format", "parquet")

    def _read_any(self, layer: str, table: str, date: str) -> pd.DataFrame:
        p = part_path(self.root, layer, table, self.chain_id, self.network, date)
        files = list(p.glob("*.parquet")) + list(p.glob("*.csv"))
        if not files: return pd.DataFrame()
        if files[0].suffix == ".parquet":
            return pd.read_parquet(files[0])
        return pd.read_csv(files[0])

    def curate(self, ingest_date: str):
        date = ingest_date

        # blocks -> block_core
        raw_blocks = self._read_any("raw", "blocks", date)
        if not raw_blocks.empty:
            out = part_path(self.root, "curated", "block_core", self.chain_id, self.network, date)
            cols = ["chain_id","network","height_or_slot","epoch","block_hash","parent_hash","proposer_index","proposer_address","timestamp_utc"]
            for c in cols:
                if c not in raw_blocks.columns: raw_blocks[c] = None
            write_rows(raw_blocks[cols].drop_duplicates(), out, self.format)

        # validators -> validator_core
        raw_vals = self._read_any("raw", "validators", date)
        if not raw_vals.empty:
            out = part_path(self.root, "curated", "validator_core", self.chain_id, self.network, date)
            cols = ["chain_id","network","snapshot_ts","validator_id","status","balance","effective_balance","slashed","withdrawal_address"]
            for c in cols:
                if c not in raw_vals.columns: raw_vals[c] = None
            write_rows(raw_vals[cols].drop_duplicates(), out, self.format)

        # attestations -> attestation_core
        raw_atts = self._read_any("raw", "attestations", date)
        if not raw_atts.empty:
            out = part_path(self.root, "curated", "attestation_core", self.chain_id, self.network, date)
            cols = ["chain_id","network","height_or_slot","epoch","committee_index","head_block_root","source_epoch","target_epoch"]
            for c in cols:
                if c not in raw_atts.columns: raw_atts[c] = None
            write_rows(raw_atts[cols].drop_duplicates(), out, self.format)

        # penalties -> penalty_core
        raw_pen = self._read_any("raw", "penalties", date)
        if not raw_pen.empty:
            out = part_path(self.root, "curated", "penalty_core", self.chain_id, self.network, date)
            cols = ["chain_id","network","height_or_slot","validator_id","penalty_type","value","meta_json"]
            for c in cols:
                if c not in raw_pen.columns: raw_pen[c] = None
            write_rows(raw_pen[cols].drop_duplicates(), out, self.format)

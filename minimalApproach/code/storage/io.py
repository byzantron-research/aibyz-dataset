from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

def write_outputs(rows: List[Dict[str, Any]], out_dir: Path, prefix: str = "validators_mvp") -> None:
    df = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{prefix}.csv"
    pq_path  = out_dir / f"{prefix}.parquet"
    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(pq_path, index=False)   # requires pyarrow or fastparquet
    except Exception:
        pass

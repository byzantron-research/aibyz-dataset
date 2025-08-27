from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

def write_outputs(rows: List[Dict[str, Any]], out_dir: Path, prefix: str = "validators_mvp") -> None:
    df = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{prefix}.csv"

    # Append to CSV if it exists
    if csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        df = pd.concat([existing_df, df], ignore_index=True)

    df.to_csv(csv_path, index=False)

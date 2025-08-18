"""Filesystem storage helpers for reading and writing partitioned datasets.

This module provides utilities to construct standardized output paths and to
write or read data to and from disk. Datasets are partitioned by layer
(``raw``, ``curated``, ``features``), table name, chain ID, network and date.

The functions in this module perform minimal validation and are designed to
gracefully handle missing data (for example, returning an empty DataFrame
when no files are present). When writing data, directories are created
automatically.
"""

from pathlib import Path
import json
from typing import List, Dict, Any, Optional
import pandas as pd

def part_path(root: Path, layer: str, table: str, chain_id: str, network: str, date: str) -> Path:
    """Construct and return a partitioned directory path, creating it if absent.

    :param root: Base directory where all data is stored. May be a :class:`pathlib.Path`
      or a string.
    :param layer: Data layer (e.g. ``"raw"``, ``"curated"``, ``"features"``).
    :param table: Table name within the layer.
    :param chain_id: Blockchain identifier (e.g. ``"eth2"``).
    :param network: Network identifier (e.g. ``"mainnet"``).
    :param date: Date partition (``YYYY‑MM‑DD``).
    :returns: A :class:`pathlib.Path` pointing to the partition directory.
    """
    base = Path(root)
    p = base / layer / table / f"chain_id={chain_id}" / f"network={network}" / f"date={date}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_rows(
    rows: List[Dict[str, Any]],
    outdir: Path,
    fmt: str = "parquet",
    filename: str = "part-000",
) -> None:
    """Persist a list of row dictionaries to disk in the specified format.

    If ``rows`` is empty the function will create a sentinel ``.empty``
    file to indicate that the dataset is empty but has been processed.
    When data is present, it is written as either CSV or Parquet without
    including an index column.

    :param rows: List of dictionaries representing rows.
    :param outdir: Output directory. Must already exist; created by
        :func:`part_path` in normal use.
    :param fmt: Output format: ``"parquet"`` (default) or ``"csv"``.
    :param filename: Base filename without extension. A suffix will be
        appended based on the format.
    :raises ValueError: If ``fmt`` is not supported.
    :raises IOError: If the file cannot be written.
    """
    df = pd.DataFrame(rows)
    if df.empty:
        # Create an explicit marker file to indicate that the dataset was
        # intentionally empty. This can be used downstream to detect the
        # absence of data versus a missing run.
        (outdir / f"{filename}.empty").write_text("", encoding="utf-8")
        return
    if fmt == "csv":
        df.to_csv(outdir / f"{filename}.csv", index=False)
    elif fmt == "parquet":
        df.to_parquet(outdir / f"{filename}.parquet", index=False)
    else:
        raise ValueError(f"Unsupported output format: {fmt}")

def write_provenance(outdir: Path, payload: Dict[str, Any], name: str = "_PROVENANCE.json") -> None:
    """Write a provenance JSON file describing a dataset generation event.

    :param outdir: Directory into which the provenance file should be written.
    :param payload: Dictionary containing provenance information. See
        :class:`common.provenance.Provenance` for a typical schema.
    :param name: Filename of the provenance file. Defaults to
        ``"_PROVENANCE.json"``.
    """
    with open(outdir / name, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def read_any(
    root: Path,
    layer: str,
    table: str,
    chain_id: str,
    network: str,
    date: str,
) -> pd.DataFrame:
    """Read any available dataset fragments (CSV or Parquet) into a single DataFrame.

    The function searches for CSV or Parquet files in the directory returned by
    :func:`part_path` and concatenates them. If no files are found or if
    all files fail to load, an empty DataFrame is returned.

    :param root: Base directory containing data.
    :param layer: Data layer (``raw``, ``curated`` or ``features``).
    :param table: Table name within the layer.
    :param chain_id: Blockchain identifier.
    :param network: Network identifier.
    :param date: Date partition.
    :returns: A :class:`pandas.DataFrame` containing the concatenated data.
    """
    # Determine partition directory; ensure it exists before scanning
    p = part_path(root, layer, table, chain_id, network, date)
    files = list(p.glob("*.parquet")) + list(p.glob("*.csv"))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            if f.suffix == ".parquet":
                dfs.append(pd.read_parquet(f))
            elif f.suffix == ".csv":
                dfs.append(pd.read_csv(f))
        except Exception:
            # Skip unreadable parts but continue scanning. This can occur if
            # another process is concurrently writing to the same directory.
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

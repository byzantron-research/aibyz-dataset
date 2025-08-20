"""Provenance metadata for dataset generation events.

The :class:`Provenance` dataclass captures metadata about a run of a
collector or curator. It is serialised to JSON and stored alongside
datasets to enable reproducibility and lineage tracking.
"""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class Provenance:
    """Encapsulates run metadata for a dataset collection or transformation.

    :param source: The API base URL or RPC endpoint used as the data source.
    :param api_version: Version string of the API or protocol.
    :param collector: Identifier of the collector or process that produced the dataset.
    :param chain_id: Blockchain identifier.
    :param network: Network identifier (e.g. ``mainnet``).
    :param dataset: Name of the dataset (e.g. ``blocks``).
    :param rows: Number of rows written.
    :param note: Optional free‑form note for additional information.
    :param schema_version: Optional schema version reference.
    """
    source: str
    api_version: str
    collector: str
    chain_id: str
    network: str
    dataset: str
    rows: int
    note: Optional[str] = None
    # Optional schema version reference (e.g. from metadata/schema_versions.json)
    schema_version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary representation suitable for JSON serialisation."""
        return asdict(self)

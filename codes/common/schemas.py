"""Pydantic models defining the core data schemas.

The models in this module correspond to the canonical row structures used
throughout the data ingestion and curation pipeline. They enforce basic
type validation and provide optional fields where information may not be
available for a particular chain or dataset.
"""

from typing import Optional
from pydantic import BaseModel

class Block(BaseModel):
    """Represents a blockchain block or slot.

    The ``epoch`` field is defined only for blockchains where the concept of
    epochs exists (e.g. Ethereum 2). The ``timestamp_utc`` field stores a
    UNIX timestamp in UTC seconds when known.
    """

    chain_id: str
    network: str
    height_or_slot: int
    epoch: Optional[int] = None
    block_hash: Optional[str] = None
    parent_hash: Optional[str] = None
    proposer_index: Optional[int] = None
    proposer_address: Optional[str] = None
    timestamp_utc: Optional[int] = None

class Validator(BaseModel):
    """Represents a validator or staking participant at a snapshot in time.

    ``snapshot_ts`` should be a UNIX timestamp in UTC seconds.
    ``validator_id`` is chain‑specific (e.g. index, operator address).
    """
    chain_id: str
    network: str
    snapshot_ts: int
    validator_id: str
    status: Optional[str] = None
    balance: Optional[float] = None
    effective_balance: Optional[float] = None
    pubkey: Optional[str] = None

class Attestation(BaseModel):
    """Represents a consensus attestation or commit.

    Fields not applicable to a particular chain may remain ``None``.
    """
    chain_id: str
    network: str
    height_or_slot: int
    epoch: Optional[int] = None
    committee_index: Optional[int] = None
    head_block_root: Optional[str] = None
    source_epoch: Optional[int] = None
    target_epoch: Optional[int] = None

class Penalty(BaseModel):
    """Represents a penalty or slashing event against a validator.

    ``meta_json`` stores the raw event or additional context as a JSON string.
    """
    chain_id: str
    network: str
    height_or_slot: int
    validator_id: Optional[str] = None
    penalty_type: Optional[str] = None
    value: Optional[int] = None
    meta_json: Optional[str] = None

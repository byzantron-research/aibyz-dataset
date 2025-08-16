from pydantic import BaseModel
from typing import Optional

class Block(BaseModel):
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
    chain_id: str
    network: str
    snapshot_ts: int
    validator_id: str
    status: Optional[str] = None
    balance: Optional[int] = None
    effective_balance: Optional[int] = None
    slashed: Optional[bool] = None
    withdrawal_address: Optional[str] = None

class Attestation(BaseModel):
    chain_id: str
    network: str
    height_or_slot: int
    epoch: Optional[int] = None
    committee_index: Optional[int] = None
    head_block_root: Optional[str] = None
    source_epoch: Optional[int] = None
    target_epoch: Optional[int] = None

class Penalty(BaseModel):
    chain_id: str
    network: str
    height_or_slot: int
    validator_id: Optional[str] = None
    penalty_type: Optional[str] = None
    value: Optional[int] = None
    meta_json: Optional[str] = None

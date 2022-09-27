from __future__ import annotations
from typing import Any, Dict, Tuple
import json


class Event:
    chain_id: int
    block_number: int
    transaction_hash: str
    log_index: int
    address: str
    event: str
    args: Dict[str, Any]

    def __init__(
        self,
        chain_id: int,
        block_number: int,
        transaction_hash: str,
        log_index: int,
        address: str,
        event: str,
        args: Dict[str, Any],
    ):
        self.chain_id = chain_id
        self.block_number = block_number
        self.transaction_hash = transaction_hash
        self.log_index = log_index
        self.address = address
        self.event = event
        self.args = args

    def from_tuple(tuple: Tuple[int, int, str, int, str, str, str]) -> Event:
        event = Event(*tuple)
        event.args = json.loads(event.args)
        return event

    def to_tuple(self) -> Tuple[int, int, str, int, str, str, str]:
        return (
            self.chain_id,
            self.block_number,
            self.transaction_hash,
            self.log_index,
            self.address,
            self.event,
            json.dumps(self.args),
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f'Event({{"chain_id":{self.chain_id}, "block_number": {self.block_number}, "transaction_hash":{self.transaction_hash}, "log_index":{self.log_index}, "address": {self.address}, "event": {self.event}, "args": {json.dumps(self.args)}}})'

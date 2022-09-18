from __future__ import annotations
from typing import Tuple


class Event:
    chain_id: int
    block_number: int
    transaction_hash: str
    log_index: int
    address: str
    event: str
    args: str

    def __init__(
        self,
        chain_id: int,
        block_number: int,
        transaction_hash: str,
        log_index: int,
        address: str,
        event: str,
        args: str,
    ):
        self.chain_id = chain_id
        self.block_number = block_number
        self.transaction_hash = transaction_hash
        self.log_index = log_index
        self.address = address
        self.event = event
        self.args = args

    def from_tuple(tuple: Tuple[int, int, str, int, str, str, str]) -> Event:
        return Event(*tuple)

    def to_tuple(self) -> Tuple[int, int, str, int, str, str, str]:
        return [
            self.chain_id,
            self.block_number,
            self.transaction_hash,
            self.log_index,
            self.address,
            self.event,
            self.args,
        ]

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f'Block({{"chain_id":{self.chain_id}, "hash": {self.hash}, "number":{self.number}, "timestamp":{self.timestamp}}})'

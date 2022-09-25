from __future__ import annotations
from typing import Tuple


class Block:
    chain_id: int
    hash: str
    number: int
    timestamp: int

    def __init__(self, chain_id: int, hash: str, number: int, timestamp: int):
        self.chain_id = chain_id
        self.hash = hash
        self.number = number
        self.timestamp = timestamp

    def from_tuple(tuple: Tuple[int, str, int, int]) -> Block:
        return Block(*tuple)

    def to_tuple(self) -> Tuple[int, str, int, int]:
        return (self.chain_id, self.hash, self.number, self.timestamp)

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f'Block({{"chain_id":{self.chain_id}, "hash": {self.hash}, "number":{self.number}, "timestamp":{self.timestamp}}})'

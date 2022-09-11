from __future__ import annotations
from typing import Tuple


class Block:
    chain_id: int
    hash: str
    number: int
    timestamp: int

    def __init__(self, *args):
        self.chain_id = args[0]
        self.hash = args[1]
        self.number = args[2]
        self.timestamp = args[3]

    def from_tuple(tuple: Tuple[int, str, int, int]) -> Block:
        return Block(*tuple)

    def to_tuple(self) -> Tuple[int, str, int, int]:
        return [self.chain_id, self.hash, self.number, self.timestamp]

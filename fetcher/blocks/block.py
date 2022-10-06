from __future__ import annotations
from typing import Any, Dict, Tuple


class Block:
    """
    Ethereum block data
    """

    #: Ethereum chain_id
    chain_id: int
    #: Block hash
    hash: str
    #: Block number
    number: int
    #: Block timestamp
    timestamp: int

    def __init__(self, chain_id: int, hash: str, number: int, timestamp: int):
        self.chain_id = chain_id
        self.hash = hash
        self.number = number
        self.timestamp = timestamp

    def from_tuple(tuple: Tuple[int, str, int, int]) -> Block:
        """
        Deserialize from database row

        Args:
            tuple: database row
        """
        return Block(*tuple)

    def to_tuple(self) -> Tuple[int, str, int, int]:
        """
        Serialize to database row

        Returns:
            database row
        """
        return (self.chain_id, self.hash, self.number, self.timestamp)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert :class:`Block` to dict
        """
        return {
            "chainId": self.chain_id,
            "hash": self.hash,
            "number": self.number,
            "timestamp": self.timestamp,
        }

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f'Block({{"chain_id":{self.chain_id}, "hash": {self.hash}, "number":{self.number}, "timestamp":{self.timestamp}}})'

from __future__ import annotations
import json
from typing import Any, Dict, Tuple


class Block:
    """
    Ethereum block data
    """

    #: Ethereum chain_id
    chain_id: int
    #: Block number
    number: int
    #: Block timestamp
    timestamp: int

    def __init__(self, chain_id: int, number: int, timestamp: int):
        self.chain_id = chain_id
        self.number = number
        self.timestamp = timestamp

    def from_row(tuple: Tuple[int, int, int]) -> Block:
        """
        Deserialize from database row

        Args:
            tuple: database row
        """
        return Block(*tuple)

    def to_row(self) -> Tuple[int, int, int]:
        """
        Serialize to database row

        Returns:
            database row
        """
        return (self.chain_id, self.number, self.timestamp)

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        """
        Create :class:`Block` from dict
        """
        return Block(
            chain_id=d["chainId"],
            number=d["number"],
            timestamp=d["timestamp"],
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert :class:`Block` to dict
        """
        return {
            "chainId": self.chain_id,
            "number": self.number,
            "timestamp": self.timestamp,
        }

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f"Block({json.dumps(self.to_dict())})"

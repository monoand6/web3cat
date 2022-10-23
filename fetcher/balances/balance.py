from __future__ import annotations
from typing import Any, Dict, Tuple
import json


class Balance:
    """
    Balance represents a snapshot of the ETH balance on Ethereum blockchain.
    """

    #: Ethereum chain_id
    chain_id: int
    #: The block number of the balance snapshot
    block_number: int
    #: The address for the ETH balance (always stored lowercase)
    address: str
    #: ETH balance
    balance: int

    def __init__(self, chain_id: int, block_number: int, address: str, balance: int):
        self.chain_id = chain_id
        self.block_number = block_number
        self.address = address.lower()
        self.balance = balance

    @staticmethod
    def from_tuple(tuple: Tuple[int, int, str, int]) -> Balance:
        """
        Deserialize from database row

        Args:
            tuple: database row
        """
        return Balance(*tuple)

    def to_tuple(self) -> Tuple[int, int, str, int]:
        """
        Serialize to database row

        Returns:
            database row
        """
        return (
            self.chain_id,
            self.block_number,
            self.address,
            self.balance,
        )

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        """
        Create :class:`Balance` from dict
        """
        return Balance(
            chain_id=d["chainId"],
            block_number=d["blockNumber"],
            address=d["address"],
            balance=d["balance"],
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert :class:`Balance` to dict
        """
        return {
            "chainId": self.chain_id,
            "blockNumber": self.block_number,
            "address": self.address,
            "balance": self.balance,
        }

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f"Balance(f{json.dumps(self.to_dict())})"

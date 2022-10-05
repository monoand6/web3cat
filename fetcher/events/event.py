from __future__ import annotations
from typing import Any, Dict, Tuple
import json


class Event:
    """
    Event represents an event log on Ethereum blockchain.
    """

    #: Ethereum chain_id
    chain_id: int
    #: The block this event appeared in
    block_number: int
    #: The hash of the transaction this event appeared in
    transaction_hash: str
    #: The log number for this event inside the transaction
    log_index: int
    _address: str
    #: Event name
    event: str
    #: Event data
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

    @staticmethod
    def from_tuple(tuple: Tuple[int, int, str, int, str, str, str]) -> Event:
        """
        Deserialize from database row

        Args:
            tuple: database row
        """
        event = Event(*tuple)
        event.args = json.loads(event.args)
        return event

    def to_tuple(self) -> Tuple[int, int, str, int, str, str, str]:
        """
        Serialize to database row

        Returns:
            database row
        """
        return (
            self.chain_id,
            self.block_number,
            self.transaction_hash,
            self.log_index,
            self.address,
            self.event,
            json.dumps(self.args),
        )

    @property
    def address(self) -> str:
        """
        The contract address this event appeared in.
        The convention is this address is always stored in lowercase.
        """
        return self._address

    @address.setter
    def address(self, val: str) -> str:
        self._address = val.lower()

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f'Event({{"chain_id":{self.chain_id}, "block_number": {self.block_number}, "transaction_hash":{self.transaction_hash}, "log_index":{self.log_index}, "address": {self.address}, "event": {self.event}, "args": {json.dumps(self.args)}}})'

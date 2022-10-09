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

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert :class:`Event` to dict
        """
        return {
            "chainId": self.chain_id,
            "blockNumber": self.block_number,
            "transactionHash": self.transaction_hash,
            "logIndex": self.log_index,
            "address": self.address,
            "event": self.event,
            "args": self.args,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        """
        Create :class:`Event` from dict
        """
        return Event(
            chain_id=d["chainId"],
            block_number=d["blockNumber"],
            transaction_hash=d["transactionHash"],
            log_index=d["logIndex"],
            address=d["address"],
            event=d["event"],
            args=d["args"],
        )

    def matches_filter(self, filter: Dict[str, Any] | None) -> bool:
        """
        Checks if :class:`Event` matches given event filter.

        Args:
            filter: Event filter

        Returns:
            ``True`` if matches, ``False`` otherwise
        """
        if filter is None or filter == {}:
            return True
        if self.args is None:
            return False
        for k in filter.keys():
            if not k in self.args:
                return False
            if not self._value_match_filter(self.args[k], filter[k]):
                return False
        return True

    def _value_match_filter(self, value, filter_value):
        # the most basic case: 2 plain values
        if not type(filter_value) is list and not type(value) is list:
            return value == filter_value
        # filter_value is a list of possible values (OR filter) and value is list
        if type(filter_value) is list and not type(value) is list:
            for ifv in filter_value:
                if ifv == value:
                    return True
        # filter value is plain value but value is list
        if not type(filter_value) is list and type(value) is list:
            return False
        # Now we have both values as lists
        # Case 1: filter_value is []. It is a plain list comparison then.
        # Doesn't make sense to supply [] as an empty list of ORs
        if len(filter_value) == 0:
            return value == filter_value

        # Case 2: filter_value is a list of lists. Then it's OR on lists
        if type(filter_value[0]) is list:
            for fv in filter_value:
                if fv == value:
                    return True
        # Case 3: filter_value is a list and value is a list
        return value == filter_value

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

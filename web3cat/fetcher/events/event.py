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
    #: The contract address this event appeared in (lowercase)
    address: str
    #: Event name
    event: str
    #: Event arguments (hexes are lowercase)
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
        self.address = address.lower()
        self.event = event
        self.args = self._lower(args)

    @staticmethod
    def from_row(row: Tuple[int, int, str, int, str, str, str]) -> Event:
        """
        Deserialize from web3cat.database row

        Args:
            row: database row
        """
        event = Event(*row)
        event.args = json.loads(event.args)
        return event

    def to_row(self) -> Tuple[int, int, str, int, str, str, str]:
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
    def from_dict(dct: Dict[str, Any]):
        """
        Create :class:`Event` from dict
        """
        return Event(
            chain_id=dct["chainId"],
            block_number=dct["blockNumber"],
            transaction_hash=dct["transactionHash"],
            log_index=dct["logIndex"],
            address=dct["address"].lower(),
            event=dct["event"],
            args=Event._lower(dct["args"]),
        )

    def matches_filter(self, event_filter: Dict[str, Any] | None) -> bool:
        """
        Checks if :class:`Event` matches given event event_filter.

        Args:
            event_filter: Event event_filter

        Returns:
            ``True`` if matches, ``False`` otherwise
        """
        if event_filter is None or event_filter == {}:
            return True
        if self.args is None:
            return False
        for k in event_filter.keys():
            if not k in self.args:
                return False
            if not self._value_match_filter(self.args[k], event_filter[k]):
                return False
        return True

    def _value_match_filter(self, value, filter_value):
        # the most basic case: 2 plain values
        if not isinstance(filter_value, list) and not isinstance(value, list):
            return value == filter_value
        # filter_value is a list of possible values (OR filter) and value is list
        if isinstance(filter_value, list) and not isinstance(value, list):
            for ifv in filter_value:
                if ifv == value:
                    return True
        # filter value is plain value but value is list
        if not isinstance(filter_value, list) and isinstance(value, list):
            return False
        # Now we have both values as lists
        # Case 1: filter_value is []. It is a plain list comparison then.
        # Doesn't make sense to supply [] as an empty list of ORs
        if len(filter_value) == 0:
            return value == filter_value

        # Case 2: filter_value is a list of lists. Then it's OR on lists
        if isinstance(filter_value[0], list):
            for fv in filter_value:
                if fv == value:
                    return True
        # Case 3: filter_value is a list and value is a list
        return value == filter_value

    @classmethod
    def _lower(cls, val: Any) -> Dict[str, Any]:
        if val is None:
            return None
        if isinstance(val, list):
            for i, v in enumerate(val):
                val[i] = cls._lower(v)
        if isinstance(val, dict):
            for k in val.keys():
                val[k] = cls._lower(val[k])
        if isinstance(val, str) and val.startswith("0x"):
            val = val.lower()
        return val

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f"Event({json.dumps(self.to_dict())})"

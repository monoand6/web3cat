from __future__ import annotations
import json
from typing import Any, Dict, Tuple
from fetcher.events_indices.index_data import EventsIndexData


class EventsIndex:
    """
    Stores data about fetched events for a specific
    :code:`chain_id`, :code:`address`, :code:`event` and
    :code:`argument_filters`.

    Args:
        chain_id: Ethereum chain_id
        address: Contract address
        event: Event name
        _args: Argument filters (filter for indexed fields of event)
        data: Index data storing blocks for which events were already fetched. See :class:`EventsIndexData` for details.
    """

    #: Ethereum chain_id
    chain_id: int
    #: Contract address
    address: str
    #: Event name
    event: str
    _args: Dict[str, Any]
    #: Index data storing blocks for which events were already fetched.
    data: EventsIndexData

    def __init__(
        self,
        chain_id: int,
        address: str,
        event: str,
        args: Dict[str, Any],
        data: EventsIndexData,
    ):
        self.chain_id = chain_id
        self.address = address
        self.event = event
        self.args = args
        self.data = data

    @staticmethod
    def from_tuple(tuple: Tuple[int, str, str, str, bytes]) -> EventsIndex:
        """
        Deserialize from database row
        """
        chain_id, address, event, args_json, raw_data = tuple
        args = json.loads(args_json)
        data = EventsIndexData.load(raw_data)
        return EventsIndex(chain_id, address, event, args, data)

    def to_tuple(self) -> Tuple[int, str, str, str, bytes]:
        """
        Serialize to database row
        """
        return (
            self.chain_id,
            self.address,
            self.event,
            json.dumps(self.args),
            self.data.dump(),
        )

    @property
    def args(self) -> Dict[str, Any] | None:
        """
        Argument filters for the event.

        Arguments filters are sorted by key, and each value (if :code:`list`)
        is sorted as well. This sorting is required to compare argument filters fast.
        """
        return self._args

    @args.setter
    def args(self, val: Dict[str, Any] | None):
        self._args = EventsIndex.normalize_args(val)

    def normalize_args(args: Dict[str, Any] | None) -> Dict[str, Any]:
        """
        Sort argument filters keys and each value (if :code:`list`).
        """
        if args is None:
            return {}
        res = {}
        for k in sorted(args.keys()):
            v = args[k]
            if type(v) is list:
                v = sorted(v)
            res[k] = v
        return res

    def step(self) -> int:
        """
        Returns :const:`contants.BLOCKS_PER_BIT`.
        """
        return self.data.step()

    def _dump_args(self) -> str:
        if self.args is None:
            return json.dumps({})
        res = {}
        for k in sorted(self.args.keys()):
            v = self.args[k]
            if type(v) is list:
                v = sorted(v)
            res[k] = v
        return json.dumps(res)

    def __repr__(self) -> str:
        return f"EventsIndex(chain_id: {self.chain_id}, address: {self.address}, event: {self.event}, args: {self.args}, data: {self.data})"

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

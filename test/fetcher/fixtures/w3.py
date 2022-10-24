import json
import os
from random import randint
from typing import Any, Dict, List
import pytest

from web3 import Web3

from fetcher.events.event import Event
from web3.exceptions import BlockNotFound
from eth_typing.encoding import HexStr

LATEST_BLOCK = 26789
INTIAL_TIME = 1438200000


class Web3EventFilterMock:
    _events: List[Dict[str, Any]]

    def __init__(self, events: List[Dict[str, Any]]):
        self._events = events

    def get_all_entries(self) -> List[Dict[str, Any]]:
        return self._events


class Web3Mock:
    _events: List[Event]
    _data: List[int]

    number_of_calls: int
    events_fetched: int
    event_name: str

    def __init__(self):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        events = json.load(open(f"{current_folder}/events.json"))
        self._events = [Event.from_dict(e) for e in events]
        self.address = "0x6b175474e89094c44da98b954eedeac495271d0f"
        self.events_fetched = 0
        self.event_name = "Transfer"

        self._data = []
        initial_time = 1438200000
        for i in range(LATEST_BLOCK + 1):
            self._data.append(initial_time)
            initial_time += randint(10, 20)
        self.number_of_calls = 0

    def createFilter(
        self, fromBlock: int, toBlock: int, argument_filters: Dict[str, Any] | None
    ) -> Web3EventFilterMock:
        events = [
            e
            for e in self._events
            if e.block_number >= fromBlock
            and e.block_number <= toBlock
            and e.matches_filter(argument_filters)
        ]
        self.events_fetched += len(events)
        return Web3EventFilterMock([e.to_dict() for e in events])

    def get_block(self, num: int | str) -> Dict[str, Any]:
        self.number_of_calls += 1
        if num == "latest":
            return self.get_raw_block(LATEST_BLOCK)
        if num > LATEST_BLOCK:
            raise BlockNotFound()
        if num < 1:
            raise ValueError("Should never query block 0")
        return self.get_raw_block(num)

    def get_raw_block(self, number: int) -> Dict[str, Any]:
        return {
            "hash": HexStr(Web3.keccak(number)).hex(),
            "number": number,
            "timestamp": self._data[number],
        }

    @property
    def eth(self):
        return self

    @property
    def chain_id(self):
        return 1


@pytest.fixture(scope="session")
def w3_mock() -> Web3:
    """
    Mock instance of Web3
    """
    return Web3Mock()

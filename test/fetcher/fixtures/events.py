from fetcher.events import Event
from sqlite3 import Connection
from typing import Any, Dict, List
import os
import json
import pytest

from fetcher.events.repo import EventsRepo


class Web3EventFilterMock:
    _events: List[Dict[str, Any]]

    def __init__(self, events: List[Dict[str, Any]]):
        self._events = events

    def get_all_entries(self) -> List[Dict[str, Any]]:
        return self._events


class Web3ContractEventMock:
    _events: List[Event]

    def __init__(self):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        events = json.load(open(f"{current_folder}/events.json"))
        self._events = [Event.from_dict(e) for e in events]

    def createFilter(
        self, from_block: int, to_block: int, args: Dict[str, Any] | None
    ) -> Web3EventFilterMock:
        events = [
            e
            for e in self._events
            if e.block_number >= from_block
            and e.block_number <= to_block
            and e.matches_filter(args)
        ]
        return Web3EventFilterMock([e.to_dict() for e in events])


@pytest.fixture
def events_repo(conn: Connection) -> EventsRepo:
    """
    Instance of events.EventsRepo
    """
    return EventsRepo(conn)


@pytest.fixture(scope="session")
def web3_event_mock() -> Web3ContractEventMock:
    """
    Instance of web3 Transfer event mock
    """
    return Web3ContractEventMock()

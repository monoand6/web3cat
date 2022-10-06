from fetcher.events import Event
from sqlite3 import Connection
from typing import Any, Dict, List
import os
import json
import pytest

from fetcher.events.repo import EventsRepo
from fetcher.events.service import EventsService
from fetcher.events_indices.repo import EventsIndicesRepo


class Web3EventFilterMock:
    _events: List[Dict[str, Any]]

    def __init__(self, events: List[Dict[str, Any]]):
        self._events = events

    def get_all_entries(self) -> List[Dict[str, Any]]:
        return self._events


class Web3ContractEventMock:
    _events: List[Event]
    events_fetched: int
    address: str
    event_name: str

    def __init__(self):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        events = json.load(open(f"{current_folder}/events.json"))
        self._events = [Event.from_dict(e) for e in events]
        self.events_fetched = 0
        self.address = "0x6b175474e89094c44da98b954eedeac495271d0f"
        self.event_name = "Transfer"

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


@pytest.fixture
def events_repo(conn: Connection) -> EventsRepo:
    """
    Instance of events.EventsRepo
    """
    return EventsRepo(conn)


@pytest.fixture
def events_service(
    events_repo: EventsRepo, events_indices_repo: EventsIndicesRepo
) -> EventsService:
    return EventsService(events_repo, events_indices_repo)


@pytest.fixture(scope="session")
def web3_event_mock() -> Web3ContractEventMock:
    """
    Instance of web3 Transfer event mock
    """
    return Web3ContractEventMock()

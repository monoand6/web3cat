from sqlite3 import Connection
import pytest

from fetcher.events_indices.repo import EventsIndicesRepo
from web3 import Web3


@pytest.fixture
def events_indices_repo(cache_path: str, w3_mock: Web3) -> EventsIndicesRepo:
    """
    Instance of events_indices.EventsIndicesRepo
    """
    return EventsIndicesRepo(cache_path=cache_path, w3=w3_mock)

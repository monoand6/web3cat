from sqlite3 import Connection
import pytest

from fetcher.events_indices.repo import EventsIndicesRepo


@pytest.fixture
def events_indices_repo(conn: Connection) -> EventsIndicesRepo:
    """
    Instance of events_indices.EventsIndicesRepo
    """
    return EventsIndicesRepo(conn)

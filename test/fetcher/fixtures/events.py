from sqlite3 import Connection
import pytest

from fetcher.events.repo import EventsRepo


@pytest.fixture
def events_repo(conn: Connection) -> EventsRepo:
    """
    Instance of events.EventsRepo
    """
    return EventsRepo(conn)

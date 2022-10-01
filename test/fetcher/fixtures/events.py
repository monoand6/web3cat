import pytest

from fetcher.db import DB
from fetcher.events.repo import EventsRepo


@pytest.fixture
def events_repo(db: DB) -> EventsRepo:
    """
    Instance of events.EventsRepo
    """
    return EventsRepo(db)

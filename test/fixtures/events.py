import pytest

from probe.db import DB
from probe.events.repo import EventsRepo


@pytest.fixture
def events_repo(db: DB) -> EventsRepo:
    """
    Instance of events.EventsRepo
    """
    return EventsRepo(db)

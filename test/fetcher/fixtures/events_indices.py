import pytest

from fetcher.db import DB
from fetcher.events_indices.repo import EventsIndicesRepo


@pytest.fixture
def events_indices_repo(db: DB) -> EventsIndicesRepo:
    """
    Instance of events_indices.EventsIndicesRepo
    """
    return EventsIndicesRepo(db)

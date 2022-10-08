from fetcher.calls import Call
from sqlite3 import Connection
from typing import Any, Dict, List
import os
import json
import pytest

from fetcher.calls.repo import CallsRepo


@pytest.fixture
def calls_repo(conn: Connection) -> CallsRepo:
    """
    Instance of calls.CallsRepo
    """
    return CallsRepo(conn)

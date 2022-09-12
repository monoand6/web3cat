import pytest
from probe.db import DB
import sqlite3


@pytest.fixture(scope="session")
def db(tmp_path_factory: pytest.TempPathFactory) -> DB:
    """
    Instance of DB per session
    """
    tmp = tmp_path_factory.mktemp("probe")
    conn = sqlite3.connect(f"{tmp}/test.db")
    return DB(conn)

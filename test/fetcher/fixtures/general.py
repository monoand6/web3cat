from sqlite3 import Connection
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


from fetcher.core import Repo
from fetcher.db import connection_from_path

import pytest
from web3 import Web3


rpc = pytest.mark.skipif(
    "TEST_WEB3_PROVIDER_URI" not in os.environ,
    reason="Rpc url is not set. Use `TEST_WEB3_PROVIDER_URI` env variable.",
)


@pytest.fixture(scope="session")
def cache_path(tmp_path_factory: pytest.TempPathFactory) -> Connection:
    """
    Temp path for cache
    """
    tmp = tmp_path_factory.mktemp("web3cat")
    return f"{tmp}/test.db"


@pytest.fixture(scope="session")
def conn(tmp_path_factory: pytest.TempPathFactory) -> Connection:
    """
    Instance of sqlite3.Connection
    """
    tmp = tmp_path_factory.mktemp("web3cat")
    return connection_from_path(f"{tmp}/test.db")


@pytest.fixture(autouse=True)
def rollback_db(cache_path: str, w3_mock: Web3):
    """
    Auto use fixture to rollback db after each test
    """
    try:
        yield
    finally:
        repo = Repo(cache_path=cache_path, w3=w3_mock)
        repo.rollback()


@pytest.fixture(scope="session")
def w3() -> Web3:
    """
    Instance of web3.Web3. RPC is defined by the TEST_WEB3_PROVIDER_URI env variable
    """
    url = os.getenv("TEST_WEB3_PROVIDER_URI")
    if not url:
        raise Exception(
            "To run test you need to setup web3 rpc url (TEST_WEB3_PROVIDER_URI env variable)"
        )
    return Web3(Web3.HTTPProvider(url))


class Web3Mock:
    @property
    def eth(self):
        return self

    @property
    def HTTPProvider(self):
        return self

    @property
    def chain_id(self):
        return 1

    @property
    def endpoint_uri(self):
        return "http://localhost:3333"

    def toChecksumAddress(self, addr: str):
        return Web3.toChecksumAddress(addr)

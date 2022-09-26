import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))


import pytest
from probe.db import DB
from web3 import Web3


rpc = pytest.mark.skipif(
    "TEST_WEB3_PROVIDER_URI" not in os.environ,
    reason="Rpc url is not set. Use `TEST_WEB3_PROVIDER_URI` env variable.",
)


@pytest.fixture(scope="session")
def db(tmp_path_factory: pytest.TempPathFactory) -> DB:
    """
    Instance of db.DB
    """
    tmp = tmp_path_factory.mktemp("probe")
    return DB.from_path(f"{tmp}/test.db")


@pytest.fixture(autouse=True)
def rollback_db(db: DB):
    """
    Auto use fixture to rollback db after each test
    """
    try:
        yield
    finally:
        db.rollback()


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

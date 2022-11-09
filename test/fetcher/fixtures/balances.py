from sqlite3 import Connection
from typing import Dict
import pytest
from web3cat.fetcher.balances.service import BalancesService

from fixtures.general import Web3

from web3cat.fetcher.balances.repo import BalancesRepo
from web3cat.fetcher.balances.balance import Balance
from web3 import Web3


@pytest.fixture
def balances_repo(cache_path: str, w3_mock: Web3) -> BalancesRepo:
    """
    Instance of db.BalancesRepo
    """
    return BalancesRepo(cache_path=cache_path, w3=w3_mock)


@pytest.fixture
def balances_service(balances_repo: BalancesRepo, w3_mock: Web3) -> BalancesService:
    """
    Instance of balances.Balances
    """
    return BalancesService(balances_repo, w3=w3_mock)

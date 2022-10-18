from sqlite3 import Connection
import pytest

from fixtures.general import Web3

# from fetcher.balances.service import BalancesService
from fetcher.balances.repo import BalancesRepo


@pytest.fixture
def balances_repo(conn: Connection) -> BalancesRepo:
    """
    Instance of db.BalancesRepo
    """
    return BalancesRepo(conn)


# @pytest.fixture
# def balances_service(balances_repo: BalancesRepo, w3: Web3) -> BalancesService:
#     """
#     Instance of balances.Balances
#     """
#     return BalancesService(balances_repo, w3)

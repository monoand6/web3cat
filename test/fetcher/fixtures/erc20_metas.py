from sqlite3 import Connection
import pytest

from web3cat.fetcher.erc20_metas.repo import ERC20MetasRepo
from web3 import Web3


@pytest.fixture
def erc20_metas_repo(cache_path: str, w3_mock: Web3) -> ERC20MetasRepo:
    """
    Instance of erc20_metas.ERC20MetasRepo
    """
    return ERC20MetasRepo(cache_path=cache_path, w3=w3_mock)

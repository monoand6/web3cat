from sqlite3 import Connection
import pytest

from fetcher.erc20_metas.repo import ERC20MetasRepo


@pytest.fixture
def erc20_metas_repo(conn: Connection) -> ERC20MetasRepo:
    """
    Instance of erc20_metas.ERC20MetasRepo
    """
    return ERC20MetasRepo(conn)

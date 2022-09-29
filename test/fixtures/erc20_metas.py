import pytest

from probe.db import DB
from probe.erc20_metas.repo import ERC20MetasRepo


@pytest.fixture
def erc20_metas_repo(db: DB) -> ERC20MetasRepo:
    """
    Instance of erc20_metas.ERC20MetasRepo
    """
    return ERC20MetasRepo(db)

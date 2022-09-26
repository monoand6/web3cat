import pytest

from fixtures.general import DB, Web3

from probe.blocks.service import BlocksService
from probe.blocks.repo import BlocksRepo


@pytest.fixture
def blocks_repo(db: DB) -> BlocksRepo:
    """
    Instance of db.BlocksRepo
    """
    return BlocksRepo(db)


@pytest.fixture
def blocks_service(blocks_repo: BlocksRepo, w3: Web3) -> BlocksService:
    """
    Instance of blocks.Blocks
    """
    return BlocksService(blocks_repo, w3)

from sqlite3 import Connection
import pytest

from fixtures.general import Web3
from fetcher.blocks.service import BlocksService
from fetcher.blocks.repo import BlocksRepo


@pytest.fixture
def blocks_repo(conn: Connection) -> BlocksRepo:
    """
    Instance of db.BlocksRepo
    """
    return BlocksRepo(conn)


@pytest.fixture
def blocks_service(blocks_repo: BlocksRepo, w3: Web3) -> BlocksService:
    """
    Instance of blocks.Blocks
    """
    return BlocksService(blocks_repo, w3)

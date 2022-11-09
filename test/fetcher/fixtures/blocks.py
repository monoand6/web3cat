import pytest
from fixtures.general import Web3
from web3cat.fetcher.blocks.service import BlocksService
from web3cat.fetcher.blocks.repo import BlocksRepo


@pytest.fixture
def blocks_repo(cache_path: str, w3_mock: Web3) -> BlocksRepo:
    """
    Instance of db.BlocksRepo
    """
    return BlocksRepo(cache_path=cache_path, w3=w3_mock)


@pytest.fixture
def blocks_service(blocks_repo: BlocksRepo, w3_mock: Web3) -> BlocksService:
    """
    Instance of blocks.BlocksService
    """
    return BlocksService(blocks_repo=blocks_repo, w3=w3_mock)

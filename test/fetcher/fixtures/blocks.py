from bisect import bisect
from random import randint
from sqlite3 import Connection
from typing import Any, Dict, List
import pytest
from eth_typing.encoding import HexStr

from fixtures.general import Web3
from fetcher.blocks.service import BlocksService
from fetcher.blocks.repo import BlocksRepo
from fetcher.blocks.block import Block
from web3.exceptions import BlockNotFound

LATEST_BLOCK = 26789
INTIAL_TIME = 1438200000


@pytest.fixture
def blocks_repo(conn: Connection) -> BlocksRepo:
    """
    Instance of db.BlocksRepo
    """
    return BlocksRepo(conn)


class Web3BlocksMock:
    _data: List[int]
    number_of_calls: int

    def __init__(self, data: Dict[int, int]):
        self._data = data
        self.number_of_calls = 0
        self.chain_id = 1

    @property
    def eth(self):
        return self

    def get_block(self, num: int | str) -> Dict[str, Any]:
        self.number_of_calls += 1
        if num == "latest":
            return self.get_raw_block(LATEST_BLOCK)
        if num > LATEST_BLOCK:
            raise BlockNotFound()
        if num < 1:
            raise ValueError("Should never query block 0")
        return self.get_raw_block(num)

    def block_right_after_timestamp(self, ts: int) -> Block:
        if ts >= self._data[LATEST_BLOCK]:
            bn = LATEST_BLOCK
            return Block(self.chain_id, bn, self._data[bn])
        bn = bisect(self._data, ts)
        if bn < 2:
            bn = 1
        return Block(self.chain_id, bn, self._data[bn])

    def get_raw_block(self, number: int) -> Dict[str, Any]:
        return {
            "hash": HexStr(Web3.keccak(number)).hex(),
            "number": number,
            "timestamp": self._data[number],
        }


@pytest.fixture(scope="session")
def web3_blocks_mock() -> Web3BlocksMock:
    data = []
    initial_time = 1438200000
    for i in range(LATEST_BLOCK + 1):
        data.append(initial_time)
        # initial_time += randint(10, 20)
        initial_time += 15

    return Web3BlocksMock(data)


@pytest.fixture
def blocks_service(
    blocks_repo: BlocksRepo, web3_blocks_mock: Web3BlocksMock
) -> BlocksService:
    """
    Instance of blocks.BlocksService
    """
    return BlocksService(blocks_repo, web3_blocks_mock)

import sys
import os
from typing import List

from fetcher.blocks.repo import BlocksRepo

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from fixtures.general import rpc
from fetcher.blocks.service import BlocksService
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import integers, lists
from web3 import Web3
import time

LATEST_BLOCK = 26789
INTIAL_TIME = 1438200000

import cProfile
import pstats
from pstats import SortKey


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    timestamp=integers(INTIAL_TIME, INTIAL_TIME + LATEST_BLOCK * 20),
    block_grid_step=integers(1, 1000),
)
def test_get_latest_block_at_timestamp(
    timestamp: int, block_grid_step: int, blocks_repo: BlocksRepo, w3_mock: Web3
) -> int:
    try:
        service = BlocksService(
            blocks_repo, block_grid_step=block_grid_step, w3=w3_mock
        )
        block = service.get_latest_block_at_timestamp(timestamp)
        if block is None:
            first_block = service.get_blocks([1])[0]
            assert timestamp < first_block.timestamp
        else:
            assert block.timestamp <= timestamp
            if block != service.latest_block:
                next_block = service.get_blocks([block.number + 1])[0]
                assert next_block.timestamp > timestamp
    finally:
        service.clear_cache()
        service.w3.number_of_calls = 0


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    numbers=lists(integers(2, LATEST_BLOCK - 1)),
    block_grid_step=integers(1, 1000),
)
def test_block_numbers_and_timestamps(
    numbers: List[int], block_grid_step: int, blocks_repo: BlocksRepo, w3_mock: Web3
) -> int:
    try:
        service = BlocksService(
            blocks_repo, block_grid_step=block_grid_step, w3=w3_mock
        )
        blocks = service.get_blocks(numbers)
        tss = [b.timestamp for b in blocks]
        blocks = service.get_blocks_by_timestamps(tss)
        nums = [b.number for b in blocks]
        assert nums == numbers
    finally:
        service.clear_cache()
        service.w3.number_of_calls = 0

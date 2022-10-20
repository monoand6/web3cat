import sys
import os

from fetcher.blocks.repo import BlocksRepo

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from fixtures.general import rpc
from fetcher.blocks.service import BlocksService
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import integers
import time

LATEST_BLOCK = 26789
INTIAL_TIME = 1438200000


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    timestamp=integers(INTIAL_TIME, INTIAL_TIME + LATEST_BLOCK * 20),
    grid_step=integers(1, 1000),
)
def test_get_block_right_after_timestamp(
    timestamp: int, grid_step: int, blocks_repo: BlocksRepo, web3_blocks_mock
) -> int:
    try:
        service = BlocksService(blocks_repo, web3_blocks_mock, grid_step)
        block = service.get_block_right_after_timestamp(timestamp)
        assert block == web3_blocks_mock.block_right_after_timestamp(timestamp)
    finally:
        service.clear_cache()
        web3_blocks_mock.number_of_calls = 0


# def test_get_block_right_after_timestamp(
#     blocks_repo: BlocksRepo, web3_blocks_mock
# ) -> int:
#     timestamp = 1438200016
#     grid_step = 1
#     service = BlocksService(blocks_repo, web3_blocks_mock, grid_step)
#     assert service.get_block_right_after_timestamp(
#         timestamp
#     ) == web3_blocks_mock.block_right_after_timestamp(timestamp)


# @rpc
# def test_get_latest_block(blocks_service: BlocksService):
#     block = blocks_service.get_latest_block()
#     assert block
#     assert time.time() - block.timestamp < 600


# @rpc
# def test_get_block(blocks_service: BlocksService):
#     block = blocks_service.get_block()
#     num = block.number - 100
#     block = blocks_service.get_block(num)
#     assert block.number == num
#     # cache
#     block = blocks_service.get_block(num)
#     assert block.number == num


# @rpc
# def test_get_block_right_after_timestamp_now(blocks_service: BlocksService):
#     now = time.time()
#     block = blocks_service.get_block_right_after_timestamp(now)
#     assert not block


# @rpc
# def test_get_block_right_after_timestamp_before_ethereum(blocks_service: BlocksService):
#     block = blocks_service.get_block_right_after_timestamp(1347788073)
#     assert block
#     assert block.number == 1


# @rpc
# def test_get_block_right_after_timestamp(blocks_service: BlocksService):
#     ts = time.time() - 86400

#     block = blocks_service.get_block_right_after_timestamp(ts)
#     prev_block = blocks_service.get_block(block.number - 1)
#     assert prev_block.timestamp < ts
#     assert block.timestamp >= ts

#     block1 = blocks_service.get_block_right_after_timestamp(block.timestamp)
#     assert block1.number == block.number

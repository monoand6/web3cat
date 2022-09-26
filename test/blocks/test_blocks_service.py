import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest
from fixtures.general import rpc
from probe.blocks.service import BlocksService
import time


@rpc
def test_get_latest_block(blocks_service: BlocksService):
    block = blocks_service.get_latest_block()
    assert block
    assert time.time() - block.timestamp < 600


@rpc
def test_get_block(blocks_service: BlocksService):
    block = blocks_service.get_block()
    num = block.number - 100
    block = blocks_service.get_block(num)
    assert block.number == num
    # cache
    block = blocks_service.get_block(num)
    assert block.number == num


@rpc
def test_get_block_right_after_timestamp_now(blocks_service: BlocksService):
    now = time.time()
    block = blocks_service.get_block_right_after_timestamp(now)
    assert not block


@rpc
def test_get_block_right_after_timestamp_before_ethereum(blocks_service: BlocksService):
    block = blocks_service.get_block_right_after_timestamp(1347788073)
    assert block
    assert block.number == 1


@rpc
def test_get_block_right_after_timestamp(blocks_service: BlocksService):
    ts = time.time() - 86400

    block = blocks_service.get_block_right_after_timestamp(ts)
    prev_block = blocks_service.get_block(block.number - 1)
    assert prev_block.timestamp < ts
    assert block.timestamp >= ts

    block1 = blocks_service.get_block_right_after_timestamp(block.timestamp)
    assert block1.number == block.number

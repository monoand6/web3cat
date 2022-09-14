import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))


from conftest import blocks, db
from probe.blocks import Blocks
import time


def test_get_latest_block(blocks: Blocks):
    block = blocks.get_latest_block()
    assert block
    assert time.time() - block.timestamp < 600


def test_get_block(blocks: Blocks):
    block = blocks.get_block()
    num = block.number - 100
    block = blocks.get_block(num)
    assert block.number == num
    # cache
    block = blocks.get_block(num)
    assert block.number == num


def test_get_block_right_after_timestamp_now(blocks: Blocks):
    now = time.time()
    block = blocks.get_block_right_after_timestamp(now)
    assert not block


def test_get_block_right_after_timestamp_before_ethereum(blocks: Blocks):
    block = blocks.get_block_right_after_timestamp(1347788073)
    assert block
    assert block.number == 1


def test_get_block_right_after_timestamp(blocks: Blocks):
    ts = time.time() - 86400

    block = blocks.get_block_right_after_timestamp(ts)
    prev_block = blocks.get_block(block.number - 1)
    assert prev_block.timestamp < ts
    assert block.timestamp >= ts

    block1 = blocks.get_block_right_after_timestamp(block.timestamp)
    assert block1.number == block.number

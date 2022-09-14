import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))


from conftest import blocks, db
from probe.blocks import Blocks
import time


def test_get_latest_block(blocks: Blocks):
    block = blocks.get_latest_block()
    assert block
    assert time.time() - block.timestamp < 60

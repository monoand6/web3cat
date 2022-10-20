from hypothesis import given
from blocks.strategies import block
from fetcher.blocks.block import Block


@given(block())
def test_block_tuples(block: Block):
    assert Block.from_tuple(block.to_tuple()) == block


@given(block())
def test_block_to_from_dict(block: Block):
    assert Block.from_dict(block.to_dict()) == block

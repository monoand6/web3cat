import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))


# from conftest import blocks_repo, db
from probe.blocks.repo import BlocksRepo
from probe.blocks.block import Block


def test_get_block_after_timestamp(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, "0x123", 31, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x62a", 27, 10702),
        Block(1, "0x72a", 28, 10703),
        Block(1, "0x82a", 29, 10704),
        Block(1, "0x92a", 30, 10705),
    ]
    blocks_repo.write_blocks(blocks)
    assert blocks_repo.get_block_after_timestamp(10701, 1) == blocks[2]
    assert blocks_repo.get_block_after_timestamp(10703, 1) == blocks[4]
    assert blocks_repo.get_block_after_timestamp(10705, 1) == blocks[-1]
    assert blocks_repo.get_block_after_timestamp(10706, 1) == blocks[0]
    assert blocks_repo.get_block_after_timestamp(10800, 1) == blocks[0]
    assert blocks_repo.get_block_after_timestamp(10801, 1) == None
    assert blocks_repo.get_block_after_timestamp(10000, 1) == blocks[1]


def test_get_block_before_timestamp(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, "0x123", 31, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x62a", 27, 10702),
        Block(1, "0x72a", 28, 10703),
        Block(1, "0x82a", 29, 10704),
        Block(1, "0x92a", 30, 10705),
    ]
    blocks_repo.write_blocks(blocks)
    assert blocks_repo.get_block_before_timestamp(10701, 1) == blocks[1]
    assert blocks_repo.get_block_before_timestamp(10703, 1) == blocks[3]
    assert blocks_repo.get_block_before_timestamp(10705, 1) == blocks[-2]
    assert blocks_repo.get_block_before_timestamp(10706, 1) == blocks[-1]
    assert blocks_repo.get_block_before_timestamp(10800, 1) == blocks[-1]
    assert blocks_repo.get_block_before_timestamp(10801, 1) == blocks[0]
    assert blocks_repo.get_block_before_timestamp(10000, 1) == None


def test_blocks_repo_write(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    assert len(blocks_repo.read_blocks(numbers, 1)) == 0
    blocks_repo.write_blocks(blocks)
    assert len(blocks_repo.read_blocks(numbers, 1)) == 3


def test_blocks_repo_write_uniq(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x123", 25, 10800),
    ]
    numbers = [b.number for b in blocks]
    blocks_repo.write_blocks(blocks)
    blocks_repo.write_blocks([Block(1, "0x52a", 23, 107)])
    assert blocks_repo.read_blocks(numbers, 1) == blocks


def test_blocks_repo_read_sorted(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    blocks_repo.write_blocks(blocks)
    assert blocks_repo.read_blocks(numbers, 1) == [blocks[1], blocks[2], blocks[0]]


def test_blocks_repo_read_chain_id(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(2, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    blocks_repo.write_blocks(blocks)
    assert len(blocks_repo.read_blocks(numbers, 1)) == 2
    assert len(blocks_repo.read_blocks(numbers, 2)) == 1


def test_blocks_repo_read_hash_and_number(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x62a", 27, 10702),
        Block(1, "0x72a", 28, 10703),
        Block(1, "0x82a", 29, 10704),
        Block(1, "0x92a", 30, 10705),
    ]
    reads = [22, "0x123", 25, "0x62a", "0x92a"]
    blocks_repo.write_blocks(blocks)
    assert len(blocks_repo.read_blocks(reads, 1)) == 4

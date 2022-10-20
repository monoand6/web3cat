import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


# from conftest import blocks_repo, db
from fetcher.blocks.repo import BlocksRepo
from fetcher.blocks.block import Block


def test_get_block_after_timestamp(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, 31, 10800),
        Block(1, 22, 10700),
        Block(1, 23, 10701),
        Block(1, 27, 10702),
        Block(1, 28, 10703),
        Block(1, 29, 10704),
        Block(1, 30, 10705),
    ]
    blocks_repo.save(blocks)
    assert blocks_repo.get_block_after_timestamp(1, 10701) == blocks[2]
    assert blocks_repo.get_block_after_timestamp(1, 10703) == blocks[4]
    assert blocks_repo.get_block_after_timestamp(1, 10705) == blocks[-1]
    assert blocks_repo.get_block_after_timestamp(1, 10706) == blocks[0]
    assert blocks_repo.get_block_after_timestamp(1, 10800) == blocks[0]
    assert blocks_repo.get_block_after_timestamp(1, 10801) == None
    assert blocks_repo.get_block_after_timestamp(1, 10000) == blocks[1]


def test_get_block_before_timestamp(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, 31, 10800),
        Block(1, 22, 10700),
        Block(1, 23, 10701),
        Block(1, 27, 10702),
        Block(1, 28, 10703),
        Block(1, 29, 10704),
        Block(1, 30, 10705),
    ]
    blocks_repo.save(blocks)
    assert blocks_repo.get_block_before_timestamp(1, 10701) == blocks[1]
    assert blocks_repo.get_block_before_timestamp(1, 10703) == blocks[3]
    assert blocks_repo.get_block_before_timestamp(1, 10705) == blocks[-2]
    assert blocks_repo.get_block_before_timestamp(1, 10706) == blocks[-1]
    assert blocks_repo.get_block_before_timestamp(1, 10800) == blocks[-1]
    assert blocks_repo.get_block_before_timestamp(1, 10801) == blocks[0]
    assert blocks_repo.get_block_before_timestamp(1, 10000) == None


def test_blocks_repo_write(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, 25, 10800),
        Block(1, 22, 10700),
        Block(1, 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    assert len(blocks_repo.find(1, numbers)) == 0
    blocks_repo.save(blocks)
    assert len(blocks_repo.find(1, numbers)) == 3


def test_blocks_repo_write_uniq(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, 22, 10700),
        Block(1, 23, 10701),
        Block(1, 25, 10800),
    ]
    numbers = [b.number for b in blocks]
    blocks_repo.save(blocks)
    blocks_repo.save([Block(1, 23, 107)])
    assert blocks_repo.find(1, numbers) == blocks


def test_blocks_repo_save_sorted(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, 25, 10800),
        Block(1, 22, 10700),
        Block(1, 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    blocks_repo.save(blocks)
    assert blocks_repo.find(1, numbers) == [blocks[1], blocks[2], blocks[0]]


def test_blocks_repo_save_chain_id(blocks_repo: BlocksRepo):
    blocks = [
        Block(1, 25, 10800),
        Block(2, 22, 10700),
        Block(1, 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    blocks_repo.save(blocks)
    assert len(blocks_repo.find(1, numbers)) == 2
    assert len(blocks_repo.find(2, numbers)) == 1

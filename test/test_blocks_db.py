import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest

from fixtures import db
from probe.db import DB, BlocksDB
from probe.model import Block


@pytest.fixture
def blocks_db(db: DB) -> BlocksDB:
    """
    Instance of BlocksDB
    """
    return BlocksDB(db)


@pytest.fixture(autouse=True)
def run_around_tests(db: DB):
    try:
        yield
    finally:
        db.rollback()


def test_get_block_after_timestamp(blocks_db: BlocksDB):
    blocks = [
        Block(1, "0x123", 31, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x62a", 27, 10702),
        Block(1, "0x72a", 28, 10703),
        Block(1, "0x82a", 29, 10704),
        Block(1, "0x92a", 30, 10705),
    ]
    blocks_db.write_blocks(blocks)
    assert blocks_db.get_block_after_timestamp(10701, 1) == blocks[2]
    assert blocks_db.get_block_after_timestamp(10703, 1) == blocks[4]
    assert blocks_db.get_block_after_timestamp(10705, 1) == blocks[-1]
    assert blocks_db.get_block_after_timestamp(10706, 1) == blocks[0]
    assert blocks_db.get_block_after_timestamp(10800, 1) == blocks[0]
    assert blocks_db.get_block_after_timestamp(10801, 1) == None
    assert blocks_db.get_block_after_timestamp(10000, 1) == blocks[1]


def test_get_block_before_timestamp(blocks_db: BlocksDB):
    blocks = [
        Block(1, "0x123", 31, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x62a", 27, 10702),
        Block(1, "0x72a", 28, 10703),
        Block(1, "0x82a", 29, 10704),
        Block(1, "0x92a", 30, 10705),
    ]
    blocks_db.write_blocks(blocks)
    assert blocks_db.get_block_before_timestamp(10701, 1) == blocks[1]
    assert blocks_db.get_block_before_timestamp(10703, 1) == blocks[3]
    assert blocks_db.get_block_before_timestamp(10705, 1) == blocks[-2]
    assert blocks_db.get_block_before_timestamp(10706, 1) == blocks[-1]
    assert blocks_db.get_block_before_timestamp(10800, 1) == blocks[-1]
    assert blocks_db.get_block_before_timestamp(10801, 1) == blocks[0]
    assert blocks_db.get_block_before_timestamp(10000, 1) == None


def test_blocks_db_write(blocks_db: BlocksDB):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    assert len(blocks_db.read_blocks(numbers, 1)) == 0
    blocks_db.write_blocks(blocks)
    assert len(blocks_db.read_blocks(numbers, 1)) == 3


def test_blocks_db_write_uniq(blocks_db: BlocksDB):
    blocks = [
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x123", 25, 10800),
    ]
    numbers = [b.number for b in blocks]
    blocks_db.write_blocks(blocks)
    blocks_db.write_blocks([Block(1, "0x52a", 23, 107)])
    assert blocks_db.read_blocks(numbers, 1) == blocks


def test_blocks_db_read_sorted(blocks_db: BlocksDB):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    blocks_db.write_blocks(blocks)
    assert blocks_db.read_blocks(numbers, 1) == [blocks[1], blocks[2], blocks[0]]


def test_blocks_db_read_chain_id(blocks_db: BlocksDB):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(2, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    blocks_db.write_blocks(blocks)
    assert len(blocks_db.read_blocks(numbers, 1)) == 2
    assert len(blocks_db.read_blocks(numbers, 2)) == 1


def test_blocks_db_read_hash_and_number(blocks_db: BlocksDB):
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
    blocks_db.write_blocks(blocks)
    assert len(blocks_db.read_blocks(reads, 1)) == 4

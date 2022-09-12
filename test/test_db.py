import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from fixtures import db
from probe.db import DB
from probe.model import Block


def test_blocks_write(db: DB):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    assert len(db.read_blocks(numbers, 1)) == 0
    db.write_blocks(blocks)
    assert len(db.read_blocks(numbers, 1)) == 3
    db.rollback()


def test_blocks_write_uniq(db: DB):
    blocks = [
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
        Block(1, "0x123", 25, 10800),
    ]
    numbers = [b.number for b in blocks]
    db.write_blocks(blocks)
    db.write_blocks([Block(1, "0x52a", 23, 107)])
    assert db.read_blocks(numbers, 1) == blocks
    db.rollback()


def test_blocks_read_sorted(db: DB):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(1, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    db.write_blocks(blocks)
    print(db.read_blocks(numbers, 1))
    assert db.read_blocks(numbers, 1) == [blocks[1], blocks[2], blocks[0]]
    db.rollback()


def test_blocks_read_chain_id(db: DB):
    blocks = [
        Block(1, "0x123", 25, 10800),
        Block(2, "0xd2f", 22, 10700),
        Block(1, "0x52a", 23, 10701),
    ]
    numbers = [b.number for b in blocks]
    db.write_blocks(blocks)
    assert len(db.read_blocks(numbers, 1)) == 2
    assert len(db.read_blocks(numbers, 2)) == 1
    db.rollback()


def test_blocks_read_hash_and_number(db: DB):
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
    db.write_blocks(blocks)
    assert len(db.read_blocks(reads, 1)) == 4
    db.rollback()

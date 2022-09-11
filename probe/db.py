from __future__ import annotations
import sqlite3
from typing import List, Tuple

from probe.model import Block


class DB:
    _conn: sqlite3.Connection

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._prepare_db()

    def from_path(path: str) -> DB:
        conn = sqlite3.connect(path)
        return DB(conn)

    def _prepare_db(self):
        cursor = self._conn.cursor()
        # Events table
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS events
                    (chain_id integer, block_number integer, transaction_hash text, log_index integer, address text, event text, args text)"""
        )
        cursor.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_events_id 
        ON events(chain_id,transaction_hash,log_index)
        """
        )
        cursor.execute(
            """CREATE INDEX IF NOT EXISTS idx_events_search
        ON events(chain_id,block_number,event,address)
        """
        )

        # Blocks table
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS blocks
                    (chain_id integer, block_hash text, block_number integer, timestamp integer)"""
        )
        cursor.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_id
                ON blocks(chain_id,block_hash,block_number)"""
        )
        self._conn.commit()

    def read_blocks(
        self, blocks: int | str | List[int | str], chain_id: int
    ) -> Tuple[List[Block]]:
        int_blocks, str_blocks = self._resolve_blocks_args(blocks)
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT * FROM blocks WHERE block_number IN ? AND chainId = ?",
            (int_blocks, chain_id),
        )
        int_blocks = cursor.fetchall()
        cursor.execute(
            "SELECT * FROM blocks WHERE block_hash IN ? AND chainId = ?",
            (str_blocks, chain_id),
        )
        str_blocks = cursor.fetchall()
        return [
            Block.from_tuple(b)
            for b in sorted(int_blocks + str_blocks, key=lambda x: x[1])
        ]

    def write_blocks(self, blocks: List[Block]):
        cursor = self._conn.cursor()
        rows = [b.to_tuple() for b in blocks]
        cursor.executemany(
            "INSERT INTO blocks VALUES(?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def _resolve_blocks_args(
        self, blocks: int | str | List[int | str]
    ) -> Tuple[List[int], List[str]]:
        if type(blocks) == int:
            return [[blocks], []]
        if type(blocks) == str:
            return [[], [blocks]]
        int_res, str_res = [], []
        for b in blocks:
            if type(b) == int:
                int_res.append(b)
                continue
            if type(blocks) == str:
                str_res.append(b)
                continue
            raise Exception(f"Unsupported block type {type(b)} for block {b}")
        return [int_res, str_res]

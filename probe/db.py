from __future__ import annotations
import sqlite3
from typing import List, Tuple
from os.path import exists
from probe.model import Block


class DB:
    _conn: sqlite3.Connection

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def from_path(path: str) -> DB:
        is_fresh = not exists(path)
        conn = sqlite3.connect(path)
        db = DB(conn)
        if is_fresh:
            DB._init_db(db)

        return db

    def _init_db(self):
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

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()


class BlocksDB:
    _db: DB

    def __init__(self, db: DB):
        self._db = db

    def read_blocks(
        self, blocks: int | str | List[int | str], chain_id: int
    ) -> Tuple[List[Block]]:
        int_blocks, str_blocks = self._resolve_blocks_args(blocks)
        cursor = self._db._conn.cursor()
        int_blocks_res = []
        str_blocks_res = []
        if len(int_blocks) > 0:
            statement = f"SELECT * FROM blocks WHERE block_number IN ({','.join('?' * len(int_blocks))}) AND chain_id = ?"
            cursor.execute(
                statement,
                int_blocks + [chain_id],
            )
            int_blocks_res = cursor.fetchall()
        if len(str_blocks) > 0:
            statement = f"SELECT * FROM blocks WHERE block_hash IN ({','.join('?' * len(str_blocks))}) AND chain_id = ?"
            cursor.execute(
                statement,
                str_blocks + [chain_id],
            )
            str_blocks_res = cursor.fetchall()
        blocks_res = int_blocks_res + str_blocks_res
        blocks_res = [Block.from_tuple(b) for b in blocks_res]
        # unique
        blocks_res = list({(b.number): b for b in blocks_res}.values())
        return sorted(blocks_res, key=lambda x: x.number)

    def write_blocks(self, blocks: List[Block]):
        cursor = self._db._conn.cursor()
        rows = [b.to_tuple() for b in blocks]
        cursor.executemany(
            "INSERT INTO blocks VALUES(?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()

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
            if type(b) == str:
                str_res.append(b)
                continue
            raise Exception(f"Unsupported block type {type(b)} for block {b}")
        return [int_res, str_res]

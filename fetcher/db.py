"""


Db module contains db class

"""

from __future__ import annotations
import sqlite3
from os.path import exists


class DB:
    """
    fetcher.db.DB
    ^^^^^^^^^^^^^
    Class for database
    """

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

        # Event indices table

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS events_indices
                    (chain_id integer, address text, event text, args text, mask blob)"""
        )
        cursor.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_events_indices_id
                ON events_indices(chain_id,address,event,args)"""
        )

        # ERC20 metas table

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS erc20_metas
                    (chain_id integer, address text, name text, symbol text, decimals integer)"""
        )
        cursor.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_erc20_metas_id
                ON erc20_metas(chain_id,address)"""
        )
        cursor.execute(
            """CREATE INDEX IF NOT EXISTS idx_erc20_metas_index
                ON erc20_metas(chain_id,address,symbol)"""
        )

    def cursor(self) -> sqlite3.Cursor:
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

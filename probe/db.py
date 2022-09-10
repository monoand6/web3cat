from __future__ import annotations
import sqlite3


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

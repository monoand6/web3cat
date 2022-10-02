from __future__ import annotations
from sqlite3 import Connection, connect
from os.path import exists


class Repo:
    """
    Base class for any repo
    """

    _connection: Connection

    def __init__(self, db: Connection):
        self._connection = db

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()


def connection_from_path(path: str) -> Connection:
    """
    Create a connection to a database at `path`.
    If the file at `path` doesn't exist, create a new one and
    initialize a database schema.

    Args:
        path: The absolute path to the database

    Returns:
        An instance of sqlite3 Connection

    Note:
        The schema migrations are currently not supported.
    """

    is_fresh = not exists(path)
    conn = connect(path)
    if is_fresh:
        _init_db(conn)

    return conn


def _init_db(conn: Connection):
    cursor = conn.cursor()
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
    conn.commit()

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

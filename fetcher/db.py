"""
This module contains base classes for working with sqlite3 db.
"""

from __future__ import annotations
from sqlite3 import Connection, connect
from os.path import exists


class Repo:
    """
    Base class for any repo used in the :mod:`fetcher` module.
    This is a thin wrapper around `sqlite3.Connection <https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection>`_ so that
    subclasses use has-a inheritance with a connection.

    Important:
        All the changes happening at the repo must be committed using
        :meth:`commit` method or rolled back using :meth:`rollback` method. Otherwise
        there's no guarantee that changes will be saved.

    Args:
        connection: Connection to an sqlite3 database

    Examples:

        ::

            class Widgets(Repo):
                def save(self, w: Widget):
                    cursor = self._connection.cursor()
                    cursor.execute("INSERT INTO widgets VALUES (...)", w)

            ws = Widgets.from_path("cache.db")
            w = Widget(...)
            w.save() # Doesn't really save anything, changes are pending
            w.commit() # Now everything is saved

    """

    _connection: Connection

    def __init__(self, db: Connection):
        self._connection = db

    def commit(self):
        """
        Commits all changes pending on the database connection.
        """
        self._connection.commit()

    def rollback(self):
        """
        Rollbacks all changes pending on the database connection.
        """
        self._connection.rollback()


def connection_from_path(path: str) -> Connection:
    """
    Creates a connection to a database at :code:`path`.
    If the file at :code:`path` doesn't exist, creates a new one and
    initializes a database schema.

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
    """
    Initialize db schema

    Args:
        conn: Connection to the database
    """
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
                (chain_id integer, address text, event text, args text, data blob)"""
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

    # Calls

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS calls
                (chain_id integer, address text, calldata text, block_number integer, response text)"""
    )
    cursor.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_id
            ON calls(chain_id,address,calldata,block_number)"""
    )

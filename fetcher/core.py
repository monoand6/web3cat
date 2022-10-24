import os

from functools import cached_property
from sqlite3 import Connection
from web3 import Web3

from fetcher.db import connection_from_path

DEFAULT_BLOCK_GRID_STEP = 1000
web3_cache = {}
db_cache = {}
chain_id_cache = {}


class Core:
    rpc: str | None
    cache: str | None
    _block_grid_step: int

    def __init__(
        self,
        rpc: str | None = None,
        cache_path: str | None = None,
        block_grid_step: int = DEFAULT_BLOCK_GRID_STEP,
        w3: Web3 | None = None,
        conn: Connection | None = None,
    ):
        self.rpc = rpc
        self.cache_path = cache_path
        self._block_grid_step = block_grid_step
        self._w3 = w3
        self._conn = conn

    @cached_property
    def block_grid_step(self) -> int:
        env_value = os.environ.get("WEB3_BLOCK_GRID_STEP")
        if not env_value is None:
            return env_value
        return self._block_grid_step

    @cached_property
    def chain_id(self):
        if not self.rpc:
            return self.w3.eth.chain_id

        if not self.rpc in chain_id_cache:
            chain_id_cache[self.rpc] = self.w3.eth.chain_id

        return chain_id_cache[self.rpc]

    @cached_property
    def w3(self) -> Web3:
        if not self._w3 is None:
            return self._w3

        if self.rpc is None:
            self.rpc = os.environ.get("WEB3_PROVIDER_URI")

        if self.rpc is None:
            raise ValueError(
                "Ethereum RPC is not set. Use `WEB3_PROVIDER_URI` env variable or pass rpc explicitly"
            )

        if not self.rpc in web3_cache:
            web3_cache[self.rpc] = Web3(Web3.HTTPProvider(self.rpc))

        return web3_cache[self.rpc]

    @cached_property
    def conn(self) -> Connection:
        if not self._conn is None:
            return self._conn

        if self.cache_path is None:
            self.cache_path = os.environ.get("WEB3_CACHE_PATH")

        if self.cache_path is None:
            raise ValueError(
                "Cache database path is not set. Use `WEB3_CACHE_PATH` env variable or pass cache_path explicitly"
            )

        if not self.cache_path in db_cache:
            db_cache[self.cache_path] = connection_from_path(self.cache_path)

        return db_cache[self.cache_path]


class Repo(Core):
    """
    Base class for any repo used in the :mod:`fetcher` module.
    This is a thin wrapper around `sqlite3.Connection <https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection>`_ so that
    subclasses use has-a inheritance with a connection.

    Important:
        All the changes happening at the repo must be committed using
        :meth:`commit` method or rolled back using :meth:`rollback` method. Otherwise
        there's no guarantee that changes will be saved.

    Examples:

        ::

            class Widgets(Repo):
                def save(self, w: Widget):
                    cursor = self.conn.cursor()
                    cursor.execute("INSERT INTO widgets VALUES (...)", w)

            ws = Widgets.from_path("cache.db")
            w = Widget(...)
            w.save() # Doesn't really save anything, changes are pending
            w.commit() # Now everything is saved

    """

    def commit(self):
        """
        Commits all changes pending on the database connection.
        """
        self.conn.commit()

    def rollback(self):
        """
        Rollbacks all changes pending on the database connection.
        """
        self.conn.rollback()

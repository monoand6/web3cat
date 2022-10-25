"""
Implements :class:`Core` that is used in other modules.
"""

import os
from os.path import exists
from sqlite3 import Connection, connect
from functools import cached_property
from web3 import Web3

DEFAULT_BLOCK_GRID_STEP = 1000
web3_cache = {}
db_cache = {}
chain_id_cache = {}


class Core:
    """
    A base class for any class that wants to use
    an Ethereum RPC or Sqlite3 cache database.

    When deriving this class, you're providing arguments like rpc url
    or OS path to the database. The resources are instantiated
    on demand though. It means that if you're just using the Ethereum
    RPC it's sufficient to supply only the rpc endpoint and skip OS path
    to the database in the constructor.

    So this class lightweight and safe to derive from any other
    class.

    **Caching**

    The web3 instance and chain_id are cached by the rpc url key.
    The sqlite3 connection is cached by the OS path of the database.

    While this might not work well in a multi-threaded scenario, for
    single-threaded there's no overhead like making new connections
    and, for example, querying chain_id each time it's accessed.

    **Block grid**

    It's often desirable to convert block number to timestamp and vice
    versa. In a way, blocks are blockchain-readable, and timestamps are
    human-readable.

    However, fetching every single block is impractical in many cases.

    That's why the following algorithm is used for timestamp estimation:

        1. We make a block number grid with a width specified by the ``block_grid_step`` parameter.
        2. For each block number, we take the two closest grid blocks (below and above).
        3. Fetch the grid blocks
        4. Assume :math:`a_n` and :math:`a_t` is a number
           and a timestamp for the block above
        5. Assume :math:`b_n` and :math:`b_t` is a number
           and a timestamp for the block below
        6. Assume :math:`c_n` and :math:`c_t` is a number
           and a timestamp for the block we're looking for
        7. :math:`w = (c_n - b_n) / (a_n - b_n)`
        8. Then :math:`c_t = b_t \\cdot (1-w) + a_t * w`

    This algorithm gives a reasonably good approximation for the block
    timestamp and considerably reduces the number of block fetches.
    For example, if we have 500 events happening in the 1000 - 2000
    block range, then we fetch only two blocks (1000, 2000) instead of 500.

    If you still want the exact precision, use
    ``block_grid_step = 1``.

    Warning:
        It's highly advisable to use a single ``block_grid_step`` for all data.
        Otherwise (in theory) the happens-before relationship might
        be violated for the data points.

    Args:
        rpc: An https Ethereum RPC endpoint uri
        cache_path: OS path to the cache database
        block_grid_step: Distance between two adjacent grid blocks
        w3: an instance of web3 (overrides rpc)
        conn: an instance of database connection (overrides cache_path)
    """

    #: An https Ethereum RPC endpoint uri.
    #: Can be ``None`` if :class:`web3.Web3` is injected directly.
    rpc: str | None
    #: OS path to the cache database.
    #: Can be ``None`` if :class:`sqlite3.Connection` is injected directly.
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
        """
        Distance between two adjacent grid blocks
        """
        env_value = os.environ.get("WEB3_BLOCK_GRID_STEP")
        if not env_value is None:
            return env_value
        return self._block_grid_step

    @cached_property
    def chain_id(self):
        """
        Chain id for the current web3 connection
        """
        if not self.rpc:
            return self.w3.eth.chain_id

        if not self.rpc in chain_id_cache:
            chain_id_cache[self.rpc] = self.w3.eth.chain_id

        return chain_id_cache[self.rpc]

    @cached_property
    def w3(self) -> Web3:
        """
        :class:`web3.Web3` instance for working with Ethereum RPC
        """
        if not self._w3 is None:
            return self._w3

        if self.rpc is None:
            self.rpc = os.environ.get("WEB3_PROVIDER_URI")

        if self.rpc is None:
            raise ValueError(
                "Ethereum RPC is not set. \
                Use `WEB3_PROVIDER_URI` env variable or pass rpc explicitly"
            )

        if not self.rpc in web3_cache:
            web3_cache[self.rpc] = Web3(Web3.HTTPProvider(self.rpc))

        return web3_cache[self.rpc]

    @cached_property
    def conn(self) -> Connection:
        """
        :class:`sqlite3.Connection` to a database cache
        """
        if not self._conn is None:
            return self._conn

        if self.cache_path is None:
            self.cache_path = os.environ.get("WEB3_CACHE_PATH")

        if self.cache_path is None:
            raise ValueError(
                "Cache database path is not set. \
                Use `WEB3_CACHE_PATH` env variable or pass cache_path explicitly"
            )

        if not self.cache_path in db_cache:
            db_cache[self.cache_path] = connection_from_path(self.cache_path)

        return db_cache[self.cache_path]


def connection_from_path(path: str) -> Connection:
    """
    Creates a connection to a database at ``path``.
    If the file at ``path`` doesn't exist, creates a new one and
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
            (chain_id integer, block_number integer, transaction_hash text, \
            log_index integer, address text, event text, args text)"""
    )
    cursor.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_events_id \
        ON events(chain_id,transaction_hash,log_index)
    """
    )
    cursor.execute(
        """CREATE INDEX IF NOT EXISTS idx_events_search \
        ON events(chain_id,block_number,event,address)
    """
    )

    # Blocks table
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS blocks
                (chain_id integer, block_number integer, timestamp integer)"""
    )
    cursor.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_id
            ON blocks(chain_id,block_number)"""
    )

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
                (chain_id integer, address text, calldata text, \
                block_number integer, response text)"""
    )
    cursor.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_id
            ON calls(chain_id,address,calldata,block_number)"""
    )

    # Balances table
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS balances
                (chain_id integer, block_number integer, address text, balance integer)"""
    )
    cursor.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_balances_id \
    ON balances(chain_id,block_number,address)
    """
    )

    conn.commit()

Technical details
=================

Architecture
------------

There are 3 layers in the framework that could be used completely
independent of each other:

#. :mod:`view` - visualize blockchain data (token supply, balances, prices, ...)
#. :mod:`data` - work with blockchain data (filter, join, export to csv, ...)
#. :mod:`fetcher` - fetch and cache raw blockchain data (events, function calls, balances, ...)

Behind each layer there's a powerful opensource Python library

.. image:: /images/web3cat_arch.png

Configuration
-------------

The configuration params can be set in two ways:

#. Provided directly into constructor of any class
#. Set globally for all classes via env variable

Params:

+-----------------+----------------------+----------------------------------------+-----------------------------------------+
| __init__ name   | env variable         | Default                                | Description                             |
+=================+======================+========================================+=========================================+
| rpc             | WEB3_PROVIDER_URI    |                                        | Ethereum (or other EVM chain) endpoint  |
+-----------------+----------------------+----------------------------------------+-----------------------------------------+
| cache_path      | WEB3_CACHE_PATH      |                                        | Path for sqlite3 database cache         |
+-----------------+----------------------+----------------------------------------+-----------------------------------------+
| block_grid_step | WEB3_BLOCK_GRID_STEP | 1000                                   | Minimum gap between fetched blocks      |
|                 |                      |                                        | (see :class:`fetcher.core.Core`)        |
+-----------------+----------------------+----------------------------------------+-----------------------------------------+
| w3              |                      | Instantiated from ``rpc`` param        | Instance of :class:`web3.Web3`          |
+-----------------+----------------------+----------------------------------------+-----------------------------------------+
| conn            |                      | Instantiated from ``cache_path`` param | Instance of :class:`sqlite3.Connection` |
+-----------------+----------------------+----------------------------------------+-----------------------------------------+

Block grid
----------

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

Caching
-------

All fetched data is logically cached inside the sqlite3 database.
The caching techiques is intelligent and allows to fetch additional
data in increments and not refecth anything at all.
For example, if you fetched ERC20 ``Transfer`` events from block 
10_000 to block 20_000, then a subsequent request from 
block 15_000 to block 21_000 will return 15_000 - 20_000 from cache 
and fetch only 20_000 - 21_000 from web3 and save to cache.
A subsequent request for blocks 15_000 - 21_000 will read
everything from cache.

Additionally, it supports nesting of the argument filters.
Imagine you

1. Fetched all Transfer events from blocks 2000 - 4000
2. Fetched Transfer events with filter :code:`{"from": "0x1234..."}` for blocks 4000 - 6000
3. Fetched Transfer events with filter :code:`{"to": "0x5678..."}` for blocks 6000 - 8000

Now when you query events with filter :code:`{"from": "0x1234...", "to": "0x5678..."}`
for blocks 2000 - 8000 cache is smart enough
to figure out that all events are already in cache and
serve the cached result.

The other good thing is that sharing cache is easy - just transfer
the sqlite3 file to another device and you have all the data.
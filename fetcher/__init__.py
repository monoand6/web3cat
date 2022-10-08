"""
Fetcher module fetches blockchain data from web3 and
caches it for subsequent queries.

Every blockchain query must use this module for efficiency.
There are several services that do exactly this:

+------------------------------------------------+---------------------------+
| Service                                        | Description               |
+================================================+===========================+
| :class:`fetcher.events.EventsService`          | Fetching Ethereum events  |
+------------------------------------------------+---------------------------+
| :class:`fetcher.calls.CallsService`            | Static calls to Ethereum  |
+------------------------------------------------+---------------------------+
| :class:`fetcher.blocks.BlocksService`          | Fetching blocks metadata  |
+------------------------------------------------+---------------------------+
| :class:`fetcher.erc20_metas.ERC20MetasService` | Fetching token metadata   |
|                                                | (decimals, symbol, name)  |
+------------------------------------------------+---------------------------+

The best way to get started is to explore these services and module
docs.
"""

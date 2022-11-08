"""
Chainlink prices datasets.

The module has two classes:

* :class:`ChainlinkData` is a price data for a pair of tokens
* :class:`ChainlinkUSDData` is a price data of a token for usd

:class:`ChainlinkUSDData` is a strictly less powerful data source.
However, it's also more lightweight than :class:`ChainlinkData`.
The latter has two :class:`ChainlinkUSDData` under the hood and
relative price tokens are determined using cross exchange rates
to USD.

Note:
    Expect the error of up to 2% in this price data (this is
    how chainlink oracles work)

Note:
    Current oracles data are generally available from mid-2021.

Examples:

    .. code::

        from datetime import datetime
        from data import ChainlinkData

        dates = [datetime(2021, 6, 1), datetime(2021, 7, 1), datetime(2021, 8, 1), datetime(2021, 9, 1)]
        tokens = ["USDC", "WETH", "WBTC"]
        chainlink_data = ChainlinkData(tokens, min(dates), max(dates))

        # Historical prices WETH/USDC
        chainlink_data.prices("WETH", "USDC", dates)

        # Historical prices WBTC/WETH
        chainlink_data.prices("WBTC", "WETH", dates)

        # Underlying USDC/USD prices
        chainlink_data.get_data("USDC").prices(dates)
"""

from data.chainlink.chainlink_data import ChainlinkUSDData, ChainlinkData

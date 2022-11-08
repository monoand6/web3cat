"""
Fetches ERC20 datasets and returns them as
`polars.Dataframe <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html>`_.

Using :class:`ERC20Data` you can:

#. Get the timeseries of the wallet balances
#. Get all transfers for the wallet
#. Get top volumes breakdown by the wallet
#. etc.

Examples:

    .. code::

        from datetime import datetime
        from web3cat.data import ERC20Data

        dates = [datetime(2021, 6, 1), datetime(2021, 7, 1), datetime(2021, 8, 1), datetime(2021, 9, 1)]
        addresses = ["0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643", "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"]
        erc20_data = ERC20Data("Dai", addresses, min(dates), max(dates))

        # All erc20 transfers for addresses
        erc20_data.transfers

        # All erc20 mints and burns
        erc20_data.emission

        # All token volumes by address
        erc20_data.volume

        # Historical total supply
        erc20_data.total_supply(dates)

        # Histrorical balances for addresses
        erc20_data.balances(addresses, dates)

"""

from data.erc20s.erc20_data import ERC20Data

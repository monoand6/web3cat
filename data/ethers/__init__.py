"""
Module for fetching ETH balances for addresses.

Examples:

    .. code::

        from datetime import datetime
        from web3cat.data import EtherData

        dates = [datetime(2021, 6, 1), datetime(2021, 7, 1), datetime(2021, 8, 1), datetime(2021, 9, 1)]
        addresses = ["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "0xf977814e90da44bfa03b6295a0616a897441acec"]
        ether_data = EtherData(min(dates), max(dates))

        # Histrorical Ether balances for addresses
        ether_data.balances(addresses, dates)

"""

from data.ethers.ether_data import EtherData

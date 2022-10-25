"""
Module for fetching and caching ETH balances from web3.

The main class of this module is :class:`BalancesService`.
It is used for fetching ETH balances from web3 and caching them
for subsequent calls.

Example:
    ::

        from web3cat.fetcher.balances import BalancesService

        addresses = [
            "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
            "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        ]
        blocks = [15700000, 15800000]
        service = BalancesService.create()
        response = service.get_balances(addresses, blocks)
        # => going for web3 rpc

        response = service.get_balances(addresses, blocks)
        # => serving from cache

        response = service.get_balance(addresses[0], blocks[0])
        # => serving from cache

"""

from fetcher.balances.balance import Balance
from fetcher.balances.repo import BalancesRepo
from fetcher.balances.service import BalancesService

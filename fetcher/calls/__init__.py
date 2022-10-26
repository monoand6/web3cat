# pylint: disable=line-too-long

"""
Module for fetching and caching contract calls.

The main class of this module is :class:`CallsService`.
It is used for making static calls to Ethereum contracts
and caching them for subsequent calls.

Example:
    ::

        from web3.auto import w3
        from web3.contract import Contract
        import json
        from web3cat.fetcher.calls import CallsService

        dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        compound_address = "0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643"
        dai_abi = [{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
        token: Contract = w3.eth.contract(address=dai_address, abi=dai_abi)

        service = CallsService.create()
        response = service.get_call(
            token.functions.balanceOf(compound_address), block_number=15632000
        )
        # => going for web3 rpc

        response = service.get_call(
            token.functions.balanceOf(compound_address), block_number=15632000
        )
        # => serving from cache
"""


from fetcher.calls.call import Call
from fetcher.calls.repo import CallsRepo
from fetcher.calls.service import CallsService

"""
Module for fetching and caching calls to web3.

The main class of this module is :class:`CallsService`.
It is used for makind static calls to web3 and caching them
for subsequent calls.

Example:
    ::

        chain_id = 1
        dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        compound_address = "0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643"
        dai_abi = json.load(...)
        token: Contract = w3.eth.contract(address=dai_address, abi=dai_abi)

        service = CallsService.create()
        response = service.get_call(
            chain_id,
            token.functions.balanceOf(compound_address), block_number=15632000
        )
        # => going for web3 rpc

        response = service.get_call(
            chain_id,
            token.functions.balanceOf(compound_address), block_number=15632000
        )
        # => serving from cache
"""


from fetcher.calls.call import Call
from fetcher.calls.repo import CallsRepo
from fetcher.calls.service import CallsService

"""
Module for fetching and caching contract calls.

The main class of this module is :class:`CallsService`.
It is used for making static calls to Ethereum contracts
and caching them for subsequent calls.

Example:
    ::

        from web3cat.fetcher.calls import CallsService
        from web3cat.fetcher.erc20_metas import ERC20MetasService

        erc20 = ERC20MetasService.create()
        dai = erc20.get("Dai")
        compound_address = "0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643"

        service = CallsService.create()
        response = service.get_call(
            dai.contract.functions.balanceOf(compound_address), block_number=15632000
        )
        # => going for web3 rpc

        response = service.get_call(
            dai.contract.functions.balanceOf(compound_address), block_number=15632000
        )
        # => serving from cache
# """


from web3cat.fetcher.calls.call import Call
from web3cat.fetcher.calls.repo import CallsRepo
from web3cat.fetcher.calls.service import CallsService

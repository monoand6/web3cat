"""
Module for fetching and caching ERC20 tokens metadata 
(name, symbol, decimals).

The main class of this module is :class:`ERC20MetaService`.
It fetches token metadata from the preloaded cache or directly
from the blockchain.

Example:
    ::

        from web3cat.fetcher.erc20_metas import ERC20MetasService

        service = ERC20MetasService.create()
        dai_meta = service.get("Dai")
        # => ERC20Meta({"chainId": 1, "address": "0x6b175474e89094c44da98b954eedeac495271d0f", "name": "dai stable coin", "symbol": "dai", "decimals": 18})
        usdc_meta = service.get("USDC")
        # => ERC20Meta({"chainId": 1, "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "name": "usd coin", "symbol": "usdc", "decimals": 6})
        weth_meta = service.get("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
        # => ERC20Meta({"chainId": 1, "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "name": "weth", "symbol": "weth", "decimals": 18})
"""

from fetcher.erc20_metas.erc20_meta import ERC20Meta
from fetcher.erc20_metas.repo import ERC20MetasRepo
from fetcher.erc20_metas.service import ERC20MetasService

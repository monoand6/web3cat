from __future__ import annotations
import json
import os
from typing import Any, Dict
from web3.contract import Contract
from web3cat.fetcher.core import Core

from web3cat.fetcher.erc20_metas.erc20_meta import ERC20Meta
from web3cat.fetcher.erc20_metas.repo import ERC20MetasRepo


class ERC20MetasService(Core):
    """
    Service for fetching ERC20 tokens metadata (name, symbol, decimals).

    The sole purpose of this service is to fetch ERC20 tokens metadata from web3,
    cache it, and read from the cache on subsequent calls.

    The exact flow goes like this

    **Request/Response flow**

    ::

                    +-------------------+                     +-------+ +---------------+ +-----------------+
                    | ERC20MetasService |                     | Web3  | | PreloadedData | | ERC20MetasRepo  |
                    +-------------------+                     +-------+ +---------------+ +-----------------+
         -------------------  |                                   |             |                  |
         | Metadata request |-|                                   |             |                  |
         |------------------| |                                   |             |                  |
                              |                                   |             |                  |
                              | Find metadata                     |             |                  |
                              |------------------------------------------------>|                  |
                              |                                   |             |                  |
                              | If cache miss: Find metadata      |             |                  |
                              |------------------------------------------------------------------->|
                              |                                   |             |                  |
                              | If cache miss: Fetch metadata     |             |                  |
                              |---------------------------------->|             |                  |
                              |                                   |             |                  |
                              | Save metadata                     |             |                  |
                              |------------------------------------------------------------------->|
        --------------------  |                                   |             |                  |
        | Metadata response |-|                                   |             |                  |
        |-------------------| |                                   |             |                  |
                              |                                   |             |                  |

    Args:
        erc20_metas_repo: :class:`ERC20MetasRepo` instance
        kwargs: Args for the :class:`fetcher.core.Core`
    """

    _cache: Dict[str, Any]
    _erc20_abi: Dict[str, Any]
    _erc20_metas_repo: ERC20MetasRepo

    def __init__(self, erc20_metas_repo: ERC20MetasRepo, **kwargs):
        super().__init__(**kwargs)
        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(f"{current_folder}/tokens.json", "r", encoding="utf-8") as f:
            self._cache = json.load(f)
        with open(f"{current_folder}/erc20_abi.json", "r", encoding="utf-8") as f:
            self._erc20_abi = json.load(f)
        self._erc20_metas_repo = erc20_metas_repo

    @staticmethod
    def create(**kwargs) -> ERC20MetasService:
        """
        Create an instance of :class:`ERC20MetasService`

        Args:
            kwargs: Args for the :class:`fetcher.core.Core`

        Returns:
            An instance of :class:`ERC20MetasService`
        """

        erc20_metas_repo = ERC20MetasRepo(**kwargs)
        return ERC20MetasService(erc20_metas_repo, **kwargs)

    def get(self, token: str) -> ERC20Meta | None:
        """
        Get metadata by token symbol or token address.

        Getting token metadata by symbol only works for cached metadata.
        The preloaded cache is large and contains major
        tokens for all evm chains.

        Args:
            token: token symbol or token address

        Returns:
            An instance of :class:`ERC20Meta` or ``None`` if not found.
        """
        token = token.lower()
        cached_token = self._get_from_cache(token)
        if not cached_token:
            cached_token = self._erc20_metas_repo.find(token)
        if cached_token:
            contract: Contract = self.w3.eth.contract(
                address=self.w3.toChecksumAddress(cached_token.address),
                abi=self._erc20_abi,
            )
            cached_token.contract = contract
            return cached_token
        if not token.startswith("0x"):
            raise ValueError(f"Could not find token `{token}`")
        contract: Contract = self.w3.eth.contract(
            address=self.w3.toChecksumAddress(token), abi=self._erc20_abi
        )
        decimals = contract.functions.decimals().call()
        name = contract.functions.name().call()
        symbol = contract.functions.symbol().call()
        meta = ERC20Meta(
            self.chain_id, token, name, symbol, decimals, contract=contract
        )
        self._erc20_metas_repo.save([meta])
        self._erc20_metas_repo.conn.commit()
        return meta

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._erc20_metas_repo.purge()
        self._erc20_metas_repo.conn.commit()

    def _get_from_cache(self, token: str) -> ERC20Meta | None:
        token = token.lower()
        chain_id = str(self.chain_id)
        if not chain_id in self._cache:
            return None
        if not token in self._cache[chain_id]:
            return None
        data = self._cache[chain_id][token]
        return ERC20Meta(
            self.chain_id,
            data["address"],
            data["name"],
            data["symbol"],
            data["decimals"],
            None,
        )

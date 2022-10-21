from __future__ import annotations
import json
import os
import numpy as np
from typing import Any, Dict, List, Tuple
from fetcher.blocks.service import DEFAULT_BLOCK_TIMESTAMP_GRID
from fetcher.erc20_metas import ERC20MetasService
from fetcher.erc20_metas import erc20_meta
from fetcher.events import EventsService, Event
from fetcher.balances import BalancesService
from fetcher.blocks import BlocksService
from fetcher.calls import CallsService
import polars as pl
from web3.contract import Contract
from web3.constants import ADDRESS_ZERO
from web3 import Web3
from datetime import datetime
import time
from web3.auto import w3 as w3auto


from fetcher.erc20_metas.erc20_meta import ERC20Meta
from fetcher.db import connection_from_path


class EtherData:
    """
    Data for a specific ERC20 token.

    When the instance of the class is created, no data is
    fetched. The class has lazy properties like :attr:`transfers`
    and :attr:`volumes` that are fetched only when accessed.

    See :mod:`data.erc20s` for examples.
    """

    _balances_service: BalancesService
    _blocks_service: BlocksService
    _w3: Web3
    _chain_id: int | None

    def __init__(
        self,
        w3: Web3,
        balances_service: BalancesService,
        blocks_service: BalancesService,
    ):
        self._w3 = w3
        self._balances_service = balances_service
        self._blocks_service = blocks_service
        self._chain_id = None

    @staticmethod
    def create(
        grid_step: int = DEFAULT_BLOCK_TIMESTAMP_GRID,
        cache_path: str = "cache.sqlite3",
        rpc: str | None = None,
    ) -> EtherData:
        """
        Create an instance of :class:`ERC20Data`

        Args:
            token: Token symbol or address
            start: Start of the erc20 data - block number or datetime (inclusive)
            end: End of the erc20 data - block number or datetime (non-inclusive)
            address_filter: Limit erc20 transfers data to only these addresses. If ``None``, all transfers are fetched
            grid_step: A grid step for resolving block timestamps. See :meth:`fetcher.blocks.BlocksService.get_block_timestamps` for details
            cache_path: path for the cache database
            rpc: Ethereum rpc url. If ``None``, `Web3 auto detection <https://web3py.savethedocs.io/en/stable/providers.html#how-automated-detection-works>`_ is used

        Returns:
            An instance of :class:`ERC20Data`
        """
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
        balances_service = BalancesService.create(cache_path, rpc)
        blocks_service = BlocksService.create(grid_step, cache_path, rpc)

        return EtherData(
            w3=w3, balances_service=balances_service, blocks_service=blocks_service
        )

    @property
    def chain_id(self) -> int:
        """
        Ethereum chain_id
        """
        if self._chain_id is None:
            self._chain_id = self._w3.eth.chain_id
        return self._chain_id

    def balances(
        self, addresses: List[str], timestamps: List[int | datetime]
    ) -> pl.DataFrame:
        timestamps = sorted(self._resolve_timetamps(timestamps))
        blocks = [
            b.number for b in self._blocks_service.get_blocks_by_timestamps(timestamps)
        ]
        balances = self._balances_service.get_balances(addresses, blocks)
        balances = [b.to_dict() for b in balances]
        return pl.DataFrame(balances)

    def _resolve_timetamps(self, timestamps: List[int | datetime]) -> List[int]:
        resolved = []
        for ts in timestamps:
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                resolved.append(int(time.mktime(ts.timetuple())))
            else:
                resolved.append(ts)
        return resolved

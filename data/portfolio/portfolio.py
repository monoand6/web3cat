from __future__ import annotations
import json
import os
import time
from typing import Any, Dict, Iterator, List
from web3 import Web3
from datetime import datetime
from fetcher.erc20_metas import ERC20MetasService
from fetcher.blocks.service import DEFAULT_BLOCK_TIMESTAMP_GRID
from fetcher.erc20_metas import ERC20MetasService
from fetcher.erc20_metas import ERC20Meta
from fetcher.events import EventsService, Event
from fetcher.blocks import BlocksService
from fetcher.calls import CallsService
import polars as pl
from web3.contract import Contract
from web3.auto import w3 as w3auto
from web3.constants import ADDRESS_ZERO
import numpy as np
from data.erc20s import ERC20Data, erc20_data
from data.chainlink import ChainlinkData, ChainlinkUSDData


class PortfolioData:
    """
    Chainlink data for a specific token.

    When the instance of the class is created, no data is
    fetched. The class has lazy properties like :attr:`updates`
    that are fetched only when accessed.

    See :mod:`data.chainlink` for examples.
    """

    _start: int | datetime
    _end: int | datetime
    _step: int

    _erc20_datas: List[ERC20Data]
    _chainlink_datas: List[ChainlinkUSDData]
    _base_chainlink_datas: List[ChainlinkUSDData]
    _num_points: int

    _balances: pl.DataFrame | None

    def __init__(
        self,
        erc20_datas: List[ERC20Data],
        chainlink_datas: List[ChainlinkData],
        base_chainlink_datas: List[ChainlinkData],
        start: datetime,
        end: datetime,
        step: int,
    ):
        self._start = start
        self._end = end
        self._step = step
        self._erc20_datas = erc20_datas
        self._chainlink_datas = chainlink_datas
        self._base_chainlink_datas = base_chainlink_datas

    @staticmethod
    def create(
        tokens: List[str],
        base_tokens: List[str],
        addresses: List[str],
        start: int | datetime,
        end: int | datetime,
        step: int = 86400,
        grid_step: int = DEFAULT_BLOCK_TIMESTAMP_GRID,
        cache_path: str = "cache.sqlite3",
        rpc: str | None = None,
    ) -> PortfolioData:
        """
        Create an instance of :class:`ChainlinkUSDData`

        Args:
            token: Token symbol or address
            start: Start of the erc20 data - block number or datetime (inclusive)
            end: End of the erc20 data - block number or datetime (non-inclusive)
            grid_step: A grid step for resolving block timestamps. See :meth:`fetcher.blocks.BlocksService.get_block_timestamps` for details
            cache_path: path for the cache database
            rpc: Ethereum rpc url. If ``None``, `Web3 auto detection <https://web3py.savethedocs.io/en/stable/providers.html#how-automated-detection-works>`_ is used

        Returns:
            An instance of :class:`ChainlinkUSDData`
        """
        erc20_datas = [
            ERC20Data.create(t, start, end, addresses, grid_step, cache_path, rpc)
            for t in tokens
        ]
        chainlink_datas = [
            ChainlinkUSDData.create(t, start, end, grid_step, cache_path, rpc)
            for t in tokens
        ]
        base_chainlink_datas = [
            ChainlinkUSDData.create(t, start, end, grid_step, cache_path, rpc)
            for t in base_tokens
        ]
        return PortfolioData(
            erc20_datas=erc20_datas,
            chainlink_datas=chainlink_datas,
            base_chainlink_datas=base_chainlink_datas,
            step=step,
            start=start,
            end=end,
        )

    def _build_timestamps(self) -> Iterator[int]:
        return range(self._start, self._end, self._step)

    def _build_balances(self) -> pl.DataFrame:
        pass

    def _get_eth_balances(self, timestamps: List[int]) -> List[np.float64]:
        pass

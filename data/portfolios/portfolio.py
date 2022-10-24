from __future__ import annotations
import json
import os
import time
from typing import Any, Dict, Iterator, List
from web3 import Web3
from datetime import datetime
from data.ethers.ether_data import EtherData
from fetcher.erc20_metas import ERC20MetasService
from fetcher.core import DEFAULT_BLOCK_GRID_STEP
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
    _tokens: List[str]
    _base_tokens: List[str]
    _addresses: List[str]

    _erc20_datas: List[ERC20Data]
    _chainlink_datas: List[ChainlinkUSDData]
    _base_chainlink_datas: List[ChainlinkUSDData]
    _ether_data: EtherData | None
    _num_points: int

    _data: pl.DataFrame | None

    def __init__(
        self,
        tokens: List[str],
        base_tokens: List[str],
        addresses: List[str],
        erc20_datas: List[ERC20Data],
        chainlink_datas: List[ChainlinkData],
        base_chainlink_datas: List[ChainlinkData],
        ether_data: EtherData | None,
        start: int | datetime,
        end: int | datetime,
        step: int,
    ):
        start, end = self._resolve_timestamps([start, end])
        self._start = start
        self._end = end
        self._step = step
        self._erc20_datas = erc20_datas
        self._chainlink_datas = chainlink_datas
        self._base_chainlink_datas = base_chainlink_datas
        self._ether_data = ether_data
        self._data = None
        self._addresses = [addr.lower() for addr in addresses]
        self._tokens = tokens
        self._base_tokens = base_tokens

    @staticmethod
    def create(
        tokens: List[str],
        base_tokens: List[str],
        addresses: List[str],
        start: int | datetime,
        end: int | datetime,
        step: int = 86400,
        grid_step: int = DEFAULT_BLOCK_GRID_STEP,
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
        tokens = [t.lower() for t in tokens]
        base_tokens = [t.lower() for t in base_tokens]
        addresses = [addr.lower() for addr in addresses]
        erc20_datas = [
            ERC20Data.create(
                token=t,
                start=start,
                end=end,
                address_filter=addresses,
                grid_step=grid_step,
                cache_path=cache_path,
                rpc=rpc,
            )
            for t in tokens
            if t != "eth"
        ]

        ether_data = None
        if "eth" in tokens:
            ether_data = EtherData.create(grid_step, cache_path, rpc)

        chainlink_datas = [
            ChainlinkUSDData.create(t, start, end, grid_step, cache_path, rpc)
            for t in tokens
        ]
        base_chainlink_datas = [
            ChainlinkUSDData.create(t, start, end, grid_step, cache_path, rpc)
            for t in base_tokens
        ]
        return PortfolioData(
            tokens=tokens,
            base_tokens=base_tokens,
            addresses=addresses,
            erc20_datas=erc20_datas,
            chainlink_datas=chainlink_datas,
            base_chainlink_datas=base_chainlink_datas,
            ether_data=ether_data,
            step=step,
            start=start,
            end=end,
        )

    def base_metas(self) -> List[ERC20Meta]:
        return [chainlink_data.meta for chainlink_data in self._chainlink_datas]

    def breakdown_by_address(self, base_token: str) -> pl.DataFrame:
        base_token = base_token.lower()
        data = self._with_total_in(self.data, base_token)[
            ["timestamp", "date", "address", f"total {base_token}"]
        ]
        dfs = data.partition_by("address", True)
        addr = dfs[0]["address"][0]
        out = dfs[0].rename({f"total {base_token}": addr})[["timestamp", "date", addr]]
        for df in dfs[1:]:
            addr = df["address"][0]
            df = df.rename({f"total {base_token}": addr})[["timestamp", addr]]
            out = out.join(df, on="timestamp", how="left")
        out = out.with_column(pl.col(self._addresses[0]).alias("total"))
        for addr in self._addresses[1:]:
            out = out.with_column(pl.col("total") + pl.col(addr).alias("total"))
        return out

    def breakdown_by_token(self, base_token: str) -> pl.DataFrame:
        base_token = base_token.lower()
        data = self.data.with_column(
            (
                pl.col(self._tokens[0])
                * pl.col(f"{self._tokens[0]} / usd")
                / pl.col(f"{base_token} / usd (base)")
            ).alias(f"{self._tokens[0]}")
        )
        for token in self._tokens[1:]:
            data = data.with_column(
                (
                    pl.col(token)
                    * pl.col(f"{token} / usd")
                    / pl.col(f"{base_token} / usd (base)")
                ).alias(f"{token}")
            )
        column_names = ["timestamp", "date"]
        column_names += [pl.col(f"{t}") for t in self._tokens]
        agg_column_names = [pl.col(f"{t}").sum() for t in self._tokens]
        data = (
            data.groupby(["timestamp", "date"])
            .agg(agg_column_names)
            .sort(["timestamp"])
        )
        data = data.with_column(pl.col(f"{self._tokens[0]}").alias("total"))
        for t in self._tokens[1:]:
            data = data.with_column((pl.col("total") + pl.col(f"{t}")).alias("total"))
        return data

    @property
    def data(self) -> pl.DataFrame:
        if self._data is None:
            self._data = self._build_data()
        return self._data

    def _with_total_in(self, data: pl.DataFrame, base_token: str) -> pl.DataFrame:
        name = f"total {base_token}"
        data = data.with_column(
            (
                pl.col(self._tokens[0])
                * pl.col(f"{self._tokens[0]} / usd")
                / pl.col(f"{base_token} / usd (base)")
            ).alias(name)
        )
        for token in self._tokens[1:]:
            data = data.with_column(
                (
                    pl.col(name)
                    + pl.col(token)
                    * pl.col(f"{token} / usd")
                    / pl.col(f"{base_token} / usd (base)")
                ).alias(name)
            )
        return data

    def _resolve_timestamps(self, timestamps: List[int | datetime]) -> List[int]:
        resolved = []
        for ts in timestamps:
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                resolved.append(int(time.mktime(ts.timetuple())))
            else:
                resolved.append(ts)
        return resolved

    def _build_data(self) -> pl.DataFrame:
        timestamps = list(range(self._start, self._end, self._step))
        data = (
            self._erc20_datas[0]
            .balances_for_addresses(self._addresses, timestamps)
            .rename({"balance": self._tokens[0]})
        )
        for i in range(1, len(self._erc20_datas)):
            erc20 = self._erc20_datas[i]
            balances = erc20.balances_for_addresses(self._addresses, timestamps)[
                ["timestamp", "address", "balance"]
            ].rename({"balance": self._tokens[i]})
            data = data.join(balances, on=["timestamp", "address"], how="left")
        if self._ether_data:
            balances = self._ether_data.balances(self._addresses, timestamps)[
                ["timestamp", "address", "balance"]
            ].rename({"balance": "eth"})
            data = data.join(balances, on=["timestamp", "address"], how="left")

        for i in range(0, len(self._chainlink_datas)):
            prices = (
                self._chainlink_datas[i]
                .prices(timestamps)[["timestamp", "price"]]
                .rename({"price": f"{self._tokens[i]} / usd"})
            )
            data = data.join(prices, on=["timestamp"], how="left")
        for i in range(0, len(self._base_chainlink_datas)):
            prices = (
                self._base_chainlink_datas[i]
                .prices(timestamps)[["timestamp", "price"]]
                .rename({"price": f"{self._base_tokens[i]} / usd (base)"})
            )
            data = data.join(prices, on=["timestamp"], how="left")

        return data

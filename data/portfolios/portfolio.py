from __future__ import annotations
from functools import cached_property
import time
from typing import List
from datetime import datetime
from web3.constants import ADDRESS_ZERO
import polars as pl
from fetcher.erc20_metas import ERC20Meta
from data.ethers.ether_data import EtherData
from data.erc20s import ERC20Data
from data.chainlink import ChainlinkData
from data.core import DataCore


class PortfolioData(DataCore):
    """
    Portfolio data (a group of tokens for a group of addresses)

    """

    _tokens: List[str]
    _base_tokens: List[str]
    _addresses: List[str]
    _numpoints: int

    _erc20_datas: List[ERC20Data]
    _chainlink_data: ChainlinkData
    _ether_data: EtherData | None

    def __init__(
        self,
        tokens: List[str],
        base_tokens: List[str],
        addresses: List[str],
        start: int | datetime,
        end: int | datetime,
        numpoints: int,
        **kwargs,
    ):
        super().__init__(start, end, **kwargs)
        if len(tokens) == 0:
            raise ValueError("Tokens list cannot be empty")
        if len(base_tokens) == 0:
            raise ValueError("Base tokens list cannot be empty")

        self._tokens = tokens
        self._base_tokens = base_tokens
        self._addresses = [addr.lower() for addr in addresses]
        self._numpoints = numpoints

        services = {
            "blocks_service": self._blocks_service,
            "balances_service": self._balances_service,
            "calls_service": self._calls_service,
            "erc20_metas_service": self._erc20_metas_service,
            "events_service": self._events_service,
        }

        self._erc20_datas = [
            ERC20Data(token, addresses, start, end, **services, **kwargs)
            for token in tokens
            if token.upper() != "ETH"
        ]
        self._chainlink_data = ChainlinkData(
            list(set(tokens + base_tokens)), start, end, **services, **kwargs
        )
        self._ether_data = None
        if "ETH" in [t.upper() for t in tokens]:
            self._ether_data = EtherData(start, end, **services, **kwargs)

    @cached_property
    def tokens(self):
        return [self._get_meta(token) for token in self._tokens]

    @cached_property
    def base_tokens(self):
        return [self._get_meta(token) for token in self._base_tokens]

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

    @cached_property
    def data(self) -> pl.DataFrame:
        step = int((self.to_block_number - self.from_block_number) / self._numpoints)
        block_numbers = list(range(self.from_block_number, self.to_block_number, step))
        data = (
            self._erc20_datas[0]
            .balances(self._addresses, block_numbers)
            .rename({"balance": self._tokens[0]})
        )
        for i in range(1, len(self._erc20_datas)):
            erc20 = self._erc20_datas[i]
            balances = erc20.balances(self._addresses, block_numbers)[
                ["block_number", "address", "balance"]
            ].rename({"balance": self._tokens[i]})
            data = data.join(balances, on=["block_number", "address"], how="left")
        if not self._ether_data is None:
            balances = self._ether_data.balances(self._addresses, block_numbers)[
                ["block_number", "address", "balance"]
            ].rename({"balance": "eth"})
            data = data.join(balances, on=["block_number", "address"], how="left")

        for token in self.tokens:
            for base_token in self.base_tokens:
                prices = self._chainlink_data.prices(
                    block_numbers, token.symbol.upper(), base_token.symbol.upper()
                )
                prices = prices[["block_number", "price"]].rename(
                    {"price": f"{token.symbol.upper()} / {base_token.symbol.upper()}"}
                )
                data = data.join(prices, on=["block_number"], how="left")

        return data

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

    def _get_meta(self, token: str) -> ERC20Meta | None:
        if token.upper() == "USD":
            return ERC20Meta(1, ADDRESS_ZERO, "USD", "USD", 6, None)
        return self._erc20_metas_service.get(token)

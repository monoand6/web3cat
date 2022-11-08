from __future__ import annotations
from functools import cached_property
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

    Arguments:
        tokens: A list of tokens to track
        base_tokens: A list of tokens for aggregation by price
        addresses: A list of addresses to track
        start: start of the data
        end: endof the data
        numpoints: number of points between ``start`` and ``end``
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

    def add_address(self, address: str | List[str]):
        """
        Add address to a portfolio.

        Arguments:
            address: an address or a list of addresses
        """
        if isinstance(address, str):
            address = [address]
        self._addresses += [a.lower() for a in address]

    def add_token(self, token: str | List[str]):
        """
        Add token to a portfolio.

        Arguments:
            token: a token or a list of tokens
        """
        if isinstance(token, str):
            token = [token]
        self._tokens += token

    def add_base_token(self, base_token: str | List[str]):
        """
        Add token to a portfolio.

        Arguments:
            base_token: a base token or a list of base tokens
        """
        if isinstance(base_token, str):
            base_token = [base_token]
        self._base_tokens += base_token

    @cached_property
    def tokens(self) -> List[ERC20Meta]:
        """
        Tracked tokens metas.
        """
        return [self._get_meta(token) for token in self._tokens]

    @cached_property
    def base_tokens(self) -> List[ERC20Meta]:
        """
        Base tokens metas.
        """
        return [self._get_meta(token) for token in self._base_tokens]

    def breakdown_by_address(self, base_token: str) -> pl.DataFrame:
        """
        Breakdown of token holdings by the owner address.

        Arguments:
            base_token: token for denomination of holdings

        Returns:
            A Dataframe with fields:

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp for the snapshot of the balance                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :class:`int`               | Number of the block                                                          |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``<address N>``      | :attr:`numpy.float64`      | Balance of the address in ``base_token``                                     |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``total``            | :attr:`numpy.float64`      | Total portfolio balance in ``base_token``                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+


        """
        base_token = self._erc20_metas_service.get(base_token)
        data = self._with_total_in(self.balances_and_prices, base_token)[
            [
                "timestamp",
                "date",
                "block_number",
                "address",
                f"total {base_token.symbol.upper()}",
            ]
        ]
        dfs = data.partition_by("address", True)
        addr = dfs[0]["address"][0]
        out = dfs[0].rename({f"total {base_token.symbol.upper()}": addr})[
            ["timestamp", "date", "block_number", addr]
        ]
        for df in dfs[1:]:
            addr = df["address"][0]
            df = df.rename({f"total {base_token.symbol.upper()}": addr})[
                ["block_number", addr]
            ]
            out = out.join(df, on="block_number", how="left")
        out = out.with_column(pl.col(self._addresses[0]).alias("total"))
        for addr in self._addresses[1:]:
            out = out.with_column(pl.col("total") + pl.col(addr).alias("total"))
        return out

    def breakdown_by_token(self, base_token: str) -> pl.DataFrame:
        """
        Breakdown of token holdings by the owner address.

        Arguments:
            base_token: token for denomination of holdings

        Returns:
            A Dataframe with fields:

        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                          | Type                       | Description                                                                  |
        +================================+============================+==============================================================================+
        | ``timestamp``                  | :attr:`numpy.int64`        | Timestamp for the snapshot of the balance                                    |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``                       | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``               | :class:`int`               | Number of the block                                                          |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``<token> (<base_token>)``     | :attr:`numpy.float64`      | Value of token in ``base_token``                                             |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``total``                      | :attr:`numpy.float64`      | Total portfolio balance in ``base_token``                                    |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        """
        base_token = self._erc20_metas_service.get(base_token)
        data = self.balances_and_prices.with_column(
            (
                pl.col(self.tokens[0].symbol.upper())
                * pl.col(
                    f"{self.tokens[0].symbol.upper()} / {base_token.symbol.upper()}"
                )
            ).alias(f"{self.tokens[0].symbol.upper()} ({base_token.symbol.upper()})")
        )
        for token in self.tokens[1:]:
            data = data.with_column(
                (
                    pl.col(token.symbol.upper())
                    * pl.col(f"{token.symbol.upper()} / {base_token.symbol.upper()}")
                ).alias(f"{token.symbol.upper()} ({base_token.symbol.upper()})")
            )
        column_names = ["timestamp", "date", "block_number"]
        agg_column_names = [
            pl.col(f"{t.symbol.upper()} ({base_token.symbol.upper()})").sum()
            for t in self.tokens
        ]
        column_names += agg_column_names
        data = (
            data.groupby(["timestamp", "date", "block_number"])
            .agg(agg_column_names)
            .sort(["timestamp"])
        )
        data = data.with_column(
            pl.col(
                f"{self.tokens[0].symbol.upper()} ({base_token.symbol.upper()})"
            ).alias("total")
        )
        for t in self.tokens[1:]:
            data = data.with_column(
                (
                    pl.col("total")
                    + pl.col(f"{t.symbol.upper()} ({base_token.symbol.upper()})")
                ).alias("total")
            )
        return data

    @cached_property
    def balances_and_prices(self) -> pl.DataFrame:
        """
        Balances and prices data.

        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                          | Type                       | Description                                                                  |
        +================================+============================+==============================================================================+
        | ``timestamp``                  | :attr:`numpy.int64`        | Timestamp for the snapshot of the balance                                    |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``                       | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``               | :class:`int`               | Number of the block                                                          |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``address``                    | :class:`str`               | Address of tokens owner                                                      |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``<token>``                    | :attr:`numpy.float64`      | Value of token in ``base_token``                                             |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``<token> / <base_token>``     | :attr:`numpy.float64`      | Price of token in terms of base token                                        |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+
        | ``total``                      | :attr:`numpy.float64`      | Total portfolio balance in ``base_token``                                    |
        +--------------------------------+----------------------------+------------------------------------------------------------------------------+

        """
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
            ].rename({"balance": "ETH"})
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

        return data.sort("block_number")

    def _with_total_in(self, data: pl.DataFrame, base_token: ERC20Meta) -> pl.DataFrame:
        name = f"total {base_token.symbol.upper()}"
        data = data.with_column(
            (
                pl.col(self.tokens[0].symbol.upper())
                * pl.col(
                    f"{self.tokens[0].symbol.upper()} / {base_token.symbol.upper()}"
                )
            ).alias(name)
        )
        for token in self.tokens[1:]:
            data = data.with_column(
                (
                    pl.col(name)
                    + pl.col(token.symbol.upper())
                    * pl.col(f"{token.symbol.upper()} / {base_token.symbol.upper()}")
                ).alias(name)
            )
        return data

    def _get_meta(self, token: str) -> ERC20Meta | None:
        if token.upper() == "USD":
            return ERC20Meta(1, ADDRESS_ZERO, "USD", "USD", 6, None)
        return self._erc20_metas_service.get(token)

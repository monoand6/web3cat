# pylint: disable=line-too-long
from __future__ import annotations
from functools import cached_property
import json
import os
import time
from typing import Any, Dict, List
from datetime import datetime
import polars as pl
from web3.contract import Contract
import numpy as np

from fetcher.erc20_metas import ERC20Meta
from fetcher.events import Event
from data.core import DataCore

RESOLVER_MAPPING = {"weth": "eth", "wbtc": "btc"}


class ChainlinkUSDData(DataCore):
    """
    Chainlink data for a specific token.

    When the instance of the class is created, no data is
    fetched. The class has lazy properties like :attr:`updates`
    that are fetched only when accessed.

    See :mod:`data.chainlink` for examples.
    """

    _token: str

    def __init__(
        self, token: str, start: int | datetime, end: int | datetime, **kwargs
    ):
        super().__init__(start, end, **kwargs)
        self._token = token

    @cached_property
    def meta(self) -> ERC20Meta:
        """
        Metadata for tokens (like name, symbol and decimals)
        """
        return self._erc20_metas_service.get(self._token)

    @cached_property
    def oracle_proxy_contract(self) -> Contract:
        """
        A reference to Chainlink oracle proxy contract
        """
        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(
            f"{current_folder}/oracle_proxy.abi.json", "r", encoding="utf-8"
        ) as f:
            abi = json.load(f)
        oracle_address = self._resolve_chainlink_address(self.meta.symbol.lower())
        if oracle_address is None:
            raise LookupError(
                f"Chainlink oracle for token `{self.meta.symbol.lower()}` "
                f"on chain with id `{self._events_service.chain_id}` is not found"
            )

        return self.w3.eth.contract(
            address=self.w3.toChecksumAddress(oracle_address), abi=abi
        )

    @cached_property
    def oracle_aggregator_contract(self) -> Contract:
        """
        A reference to Chainlink oracle aggregator contract
        """

        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(
            f"{current_folder}/oracle_aggregator.abi.json", "r", encoding="utf-8"
        ) as f:
            abi = json.load(f)
        oracle_address = self._calls_service.get_call(
            self.oracle_proxy_contract.functions.aggregator(),
            self.to_block_number,
        ).response
        return self.w3.eth.contract(
            address=self.w3.toChecksumAddress(oracle_address),
            abi=abi,
        )

    @cached_property
    def oracle_decimals(self) -> int:
        """
        Chainlink oracle decimals
        """
        return int(
            self._calls_service.get_call(
                self.oracle_proxy_contract.functions.decimals(),
                self.to_block_number,
            ).response
        )

    @cached_property
    def initial_price(self) -> np.float64:
        """
        Oracle price at from_block
        """
        price = self._calls_service.get_call(
            self.oracle_aggregator_contract.functions.latestRoundData(),
            self.from_block_number,
        ).response[1]
        return price / 10**self.oracle_decimals

    @cached_property
    def index(self):
        """
        Oracles index
        """
        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(f"{current_folder}/oracles.json", "r", encoding="utf-8") as f:
            return json.load(f)

    def prices(self, timepoints: List[int | datetime]) -> pl.DataFrame:
        """
        Chainlink price feed.

        Arguments:
            timepoints: A list of timepoints for prices
        """

        blocks = self._resolve_timepoints(sorted(timepoints))
        updates = self.updates[["block_number", "price"]].to_dicts()
        i = 0
        price_list = []
        while len(updates) > 0 and blocks[i].number < updates[0]["block_number"]:
            price_list.append(self.initial_price)
            i += 1
            if i == len(blocks):
                break
        j = 0
        for b in blocks[i:]:
            while j < len(updates) and updates[j]["block_number"] <= b.number:
                j += 1
            # now ts < updates[j]
            idx = max([j - 1, 0])
            price_list.append(updates[idx]["price"])
        out = [
            {
                "timestamp": b.timestamp,
                "date": datetime.fromtimestamp(b.timestamp),
                "block_number": b.number,
                "price": price,
            }
            for b, price in zip(blocks, price_list)
        ]
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "price": pl.Float64,
            },
        )

    @cached_property
    def updates(self) -> pl.DataFrame:
        """
        A list of oracle updates
        """
        events: List[Event] = self._events_service.get_events(
            self.oracle_aggregator_contract.events.AnswerUpdated,
            self.from_block_number,
            self.to_block_number,
        )

        block_numbers = [e.block_number for e in events]
        blocks = self._blocks_service.get_blocks(block_numbers)
        timestamps = [b.timestamp for b in blocks]
        ts_index = {
            block_number: timestamp
            for block_number, timestamp in zip(block_numbers, timestamps)
        }
        factor = 10**self.oracle_decimals
        return pl.DataFrame(
            [self._event_to_row(e, ts_index[e.block_number], factor) for e in events],
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "transaction_hash": pl.Utf8,
                "log_index": pl.UInt64,
                "round": pl.UInt64,
                "updated_at": pl.UInt64,
                "price": pl.Float64,
            },
        ).sort(pl.col("timestamp"))

    def _event_to_row(self, ev: Event, ts: int, val_factor: int) -> Dict[str, Any]:
        return {
            "timestamp": ts,
            "date": datetime.fromtimestamp(ts),
            "block_number": ev.block_number,
            "transaction_hash": ev.transaction_hash,
            "log_index": ev.log_index,
            "round": ev.args["roundId"],
            "updated_at": ev.args["updatedAt"],
            "price": ev.args["current"] / val_factor,
        }

    def _resolve_chainlink_address(self, token: str) -> str | None:
        cid = str(self._events_service.chain_id)
        if not cid in self.index:
            return None
        oracles = self.index[cid]
        if token in oracles:
            return oracles[token]["address"]
        if token in RESOLVER_MAPPING:
            token = RESOLVER_MAPPING[token]
        if not token in oracles:
            return None
        return oracles[token]["address"]

    def _resolve_timestamps(self, timestamps: List[int | datetime]) -> List[int]:
        resolved = []
        for ts in timestamps:
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                resolved.append(int(time.mktime(ts.timetuple())))
            else:
                resolved.append(ts)
        return resolved


class ChainlinkData(DataCore):
    _datas: Dict[str, ChainlinkUSDData]
    _kwargs: Dict[str, Any]

    def __init__(
        self,
        tokens: List[str],
        start: int | datetime,
        end: int | datetime,
        **kwargs,
    ):
        super().__init__(start, end, **kwargs)
        self._datas = {}
        self._kwargs = kwargs
        for token in tokens:
            self.add_token(token)

    def add_token(
        self,
        token: str,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        proxy: str | None = None,
    ):
        """
        Add token to prices dataset.

        Arguments:
            token: token to add
            start: start of the data
            end: end of the data
        """
        meta = self._erc20_metas_service.get(token)
        if proxy is None:
            self._datas[meta.address] = ChainlinkUSDData(
                token, start or self.start, end or self.end, **self._kwargs
            )
        else:
            proxy_meta = self._erc20_metas_service.get(token)
            self._datas[meta.address] = self._datas[proxy_meta.address]

    def get_data(self, token: str) -> ChainlinkUSDData:
        """
        Get chainlink USD data for token.

        Agruments:
            token: ERC20 token

        Returns:
            Chainlink USD data for token
        """
        meta = self._erc20_metas_service.get(token)
        if not meta.address in self._datas:
            self.add_token(token)
        return self._datas[meta.address]

    def prices(
        self, timepoints: List[int | datetime], token0: str, token1: str
    ) -> List[np.float64]:
        """
        Chainlink prices for pair of tokens.
        The token0 = "WETH", token1 = "USDC" would mean the regular WETH/USDC.
        (that is how much dollars we give for 1 ETH)

        Dataframe with balances for addresses over time.

        Args:
            addresses: The list of addresses
            timepoints: A list of timepoints (see :class:`ERC20Data`).

        Returns a Dataframe with fields

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp for the snapshot of the balance                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :class:`int`               | Number of the block                                                          |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``balance``          | :attr:`numpy.float64`      | Balance of an address at the time                                            |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        blocks = self._resolve_timepoints(timepoints)
        if token0.upper() == "USD" and token1.upper() == "USD":
            prices = [1.0] * len(timepoints)
        elif token0.upper() == "USD":
            data = self.get_data(token1)
            prices = [1.0 / p for p in data.prices(timepoints)["price"]]
        elif token1.upper() == "USD":
            data = self.get_data(token0)
            prices = data.prices(timepoints)["price"]
        else:
            token0_data = self.get_data(token0)
            token1_data = self.get_data(token1)
            block_numbers = [b.number for b in blocks]
            prices0 = token0_data.prices(block_numbers)["price"].to_list()
            prices1 = token1_data.prices(block_numbers)["price"].to_list()
            prices = [p0 / p1 for p0, p1 in zip(prices0, prices1)]

        out = [
            {
                "timestamp": b.timestamp,
                "block_number": b.number,
                "date": datetime.fromtimestamp(b.timestamp),
                "price": p,
            }
            for b, p in zip(blocks, prices)
        ]
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "price": pl.Float64,
            },
        )

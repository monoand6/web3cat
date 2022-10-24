from __future__ import annotations
import json
import os
import time
from typing import Any, Dict, List
from web3 import Web3
from datetime import datetime
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

from fetcher.utils import get_chain_id

RESOLVER_MAPPING = {"weth": "eth", "wbtc": "btc"}


class ChainlinkUSDData:
    """
    Chainlink data for a specific token.

    When the instance of the class is created, no data is
    fetched. The class has lazy properties like :attr:`updates`
    that are fetched only when accessed.

    See :mod:`data.chainlink` for examples.
    """

    UPDATE_SCHEMA = {
        "timestamp": pl.UInt64,
        "date": pl.Datetime,
        "block_number": pl.UInt64,
        "transaction_hash": pl.Utf8,
        "log_index": pl.UInt64,
        "round": pl.UInt64,
        "updated_at": pl.UInt64,
        "price": pl.Float64,
    }

    PRICE_SCHEMA = {
        "timestamp": pl.UInt64,
        "date": pl.Datetime,
        "price": pl.Float64,
    }

    _from_block: int | None
    _to_block: int | None
    _from_date: datetime | None
    _to_date: datetime | None

    _token: str
    _erc20_metas_service: ERC20MetasService
    _events_service: EventsService
    _blocks_service: BlocksService
    _calls_service: CallsService

    _meta: ERC20Meta | None
    _oracle_decimals: int | None
    _updates: pl.DataFrame | None
    _oracle_proxy_contract: Contract | None
    _oracle_aggregator_contract: Contract | None
    _index: Dict[str, Any] | None
    _initial_price: np.float64 | None

    def __init__(
        self,
        erc20_metas_service: ERC20MetasService,
        events_service: EventsService,
        blocks_service: BlocksService,
        calls_service: CallsService,
        token: str,
        start: int | datetime,
        end: int | datetime,
    ):
        self._token = token
        if type(start) is datetime:
            self._from_date = start
        else:
            self._from_block = start
        if type(end) is datetime:
            self._to_date = end
        else:
            self._to_block = end
        self._erc20_metas_service = erc20_metas_service
        self._events_service = events_service
        self._blocks_service = blocks_service
        self._calls_service = calls_service

        self._meta = None
        self._updates = None
        self._oracle_decimals = None
        self._oracle_proxy_contract = None
        self._oracle_aggregator_contract = None
        self._index = None
        self._initial_price = None

    @staticmethod
    def create(
        token: str, start: int | datetime, end: int | datetime, **kwargs
    ) -> ChainlinkUSDData:
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
        events_service = EventsService.create(**kwargs)
        blocks_service = BlocksService.create(**kwargs)
        calls_service = CallsService.create(**kwargs)
        erc20_metas_service = ERC20MetasService.create(**kwargs)

        return ChainlinkUSDData(
            erc20_metas_service=erc20_metas_service,
            events_service=events_service,
            blocks_service=blocks_service,
            calls_service=calls_service,
            token=token,
            start=start,
            end=end,
            **kwargs,
        )

    @property
    def meta(self) -> ERC20Meta:
        """
        Metadata for tokens (like name, symbol and decimals)
        """
        if self._meta is None:
            self._meta = self._erc20_metas_service.get(self._token)
        return self._meta

    @property
    def from_block(self):
        """
        Start block for the data.
        """
        if not hasattr(self, "_from_block"):
            ts = time.mktime(self._from_date.timetuple())
            self._from_block = self._blocks_service.get_blocks_by_timestamps(ts)[
                0
            ].number
        return self._from_block

    @property
    def to_block(self):
        """
        End block for the data.
        """
        if not hasattr(self, "_to_block"):
            ts = time.mktime(self._to_date.timetuple())
            self._to_block = self._blocks_service.get_blocks_by_timestamps(ts)[0].number
        return self._to_block

    @property
    def oracle_proxy_contract(self) -> Contract:
        if self._oracle_proxy_contract is None:
            current_folder = os.path.realpath(os.path.dirname(__file__))
            with open(f"{current_folder}/oracle_proxy.abi.json", "r") as f:
                abi = json.load(f)
            oracle_address = self._resolve_chainlink_address(self.meta.symbol.lower())
            if oracle_address is None:
                raise LookupError(
                    f"Chainlink oracle for token `{self.meta.symbol.lower()}` on chain with id `{self._events_service.chain_id}` is not found"
                )

            self._oracle_proxy_contract = w3auto.eth.contract(
                address=w3auto.toChecksumAddress(oracle_address), abi=abi
            )
        return self._oracle_proxy_contract

    @property
    def oracle_aggregator_contract(self) -> Contract:
        current_folder = os.path.realpath(os.path.dirname(__file__))
        if self._oracle_aggregator_contract is None:
            with open(f"{current_folder}/oracle_aggregator.abi.json", "r") as f:
                abi = json.load(f)
            oracle_address = self._calls_service.get_call(
                self.oracle_proxy_contract.functions.aggregator(),
                self.to_block,
            ).response
            self._oracle_aggregator_contract = w3auto.eth.contract(
                address=w3auto.toChecksumAddress(oracle_address), abi=abi
            )
        return self._oracle_aggregator_contract

    @property
    def oracle_decimals(self) -> int:
        if self._oracle_decimals is None:
            self._oracle_decimals = int(
                self._calls_service.get_call(
                    self.oracle_proxy_contract.functions.decimals(),
                    self.to_block,
                ).response
            )
        return self._oracle_decimals

    @property
    def initial_price(self) -> np.float64:
        if self._initial_price is None:
            price = self._calls_service.get_call(
                self._oracle_aggregator_contract.functions.latestRoundData(),
                self.from_block,
            ).response[1]
            self._initial_price = price / 10**self.oracle_decimals
        return self._initial_price

    def prices(self, timestamps: List[int | datetime]) -> pl.DataFrame:
        timestamps = self._resolve_timetamps(timestamps)
        timestamps = sorted(timestamps)
        updates = self.updates[["timestamp", "price"]].to_dicts()
        i = 0
        price_list = []
        while timestamps[i] < updates[0]["timestamp"]:
            price_list.append(self.initial_price)
            i += 1
            if i == len(timestamps):
                break
        j = 0
        for ts in timestamps[i:]:
            while j < len(updates) and updates[j]["timestamp"] <= ts:
                j += 1
            # now ts < updates[j]
            idx = max([j - 1, 0])
            price_list.append(updates[idx]["price"])
        out = [
            {
                "timestamp": ts,
                "date": datetime.fromtimestamp(ts),
                "price": price,
            }
            for ts, price in zip(timestamps, price_list)
        ]
        return pl.DataFrame(out, ChainlinkUSDData.PRICE_SCHEMA)

    @property
    def updates(self) -> pl.DataFrame:
        if self._updates is None:
            self._updates = self._build_updates()
        return self._updates

    def _build_updates(self) -> pl.DataFrame:
        events: List[Event] = self._events_service.get_events(
            self.oracle_aggregator_contract.events.AnswerUpdated,
            self.from_block,
            self.to_block,
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
            ChainlinkUSDData.UPDATE_SCHEMA,
        ).sort(pl.col("timestamp"))

    def _event_to_row(self, e: Event, ts: int, val_factor: int) -> Dict[str, Any]:
        return {
            "timestamp": ts,
            "date": datetime.fromtimestamp(ts),
            "block_number": e.block_number,
            "transaction_hash": e.transaction_hash,
            "log_index": e.log_index,
            "round": e.args["roundId"],
            "updated_at": e.args["updatedAt"],
            "price": e.args["current"] / val_factor,
        }

    def _resolve_chainlink_address(self, token: str) -> str | None:
        if not self._index:
            current_folder = os.path.realpath(os.path.dirname(__file__))
            with open(f"{current_folder}/oracles.json", "r") as f:
                self._index = json.load(f)
        cid = str(self._events_service.chain_id)
        if not cid in self._index:
            return None
        oracles = self._index[cid]
        if token in oracles:
            return oracles[token]["address"]
        if token in RESOLVER_MAPPING:
            token = RESOLVER_MAPPING[token]
        if not token in oracles:
            return None
        return oracles[token]["address"]

    def _resolve_timetamps(self, timestamps: List[int | datetime]) -> List[int]:
        resolved = []
        for ts in timestamps:
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                resolved.append(int(time.mktime(ts.timetuple())))
            else:
                resolved.append(ts)
        return resolved


class ChainlinkData:
    _token0_data: ChainlinkUSDData
    _token1_data: ChainlinkUSDData

    _updates: pl.DataFrame | None

    def __init__(
        self, token0_data: ChainlinkUSDData, token1_data: ChainlinkUSDData | None
    ):
        self._token0_data = token0_data
        self._token1_data = token1_data
        self._updates = None

    @staticmethod
    def create(
        token0: str, token1: str, start: int | datetime, end: int | datetime, **kwargs
    ) -> ChainlinkUSDData:
        """
        Create an instance of :class:`ChainlinkData`

        Args:
            token0: Numerator token in (token0 / token1) price
            token1: Denominator token in (token0 / token1) price
            start: Start of the erc20 data - block number or datetime (inclusive)
            end: End of the erc20 data - block number or datetime (non-inclusive)
            grid_step: A grid step for resolving block timestamps. See :meth:`fetcher.blocks.BlocksService.get_block_timestamps` for details
            cache_path: path for the cache database
            rpc: Ethereum rpc url. If ``None``, `Web3 auto detection <https://web3py.savethedocs.io/en/stable/providers.html#how-automated-detection-works>`_ is used

        Returns:
            An instance of :class:`ChainlinkUSDData`
        """

        token0_data = ChainlinkUSDData.create(
            token=token0, start=start, end=end, **kwargs
        )
        token0_data = None
        token1_data = None
        if token0.lower() != "usd":
            token0_data = ChainlinkUSDData.create(
                token=token0, start=start, end=end, **kwargs
            )

        if token1.lower() != "usd":
            token1_data = ChainlinkUSDData.create(
                token=token1, start=start, end=end, **kwargs
            )

        return ChainlinkData(token0_data, token1_data)

    @property
    def updates(self) -> pl.DataFrame:
        if self._updates is None:
            self._updates = self._build_updates()
        return self._updates

    @property
    def initial_price(self):
        token0_price = (
            1 if self._token0_data is None else self._token0_data.initial_price
        )
        token1_price = (
            1 if self._token1_data is None else self._token1_data.initial_price
        )
        return token1_price / token0_price

    @property
    def token0_meta(self):
        if self._token0_data is None:
            return ERC20Meta(
                self._token0_data._events_service.chain_id,
                ADDRESS_ZERO,
                "USD",
                "USD",
                6,
            )
        return self._token0_data.meta

    @property
    def token1_meta(self):
        if self._token1_data is None:
            return ERC20Meta(
                self._token1_data._events_service.chain_id,
                ADDRESS_ZERO,
                "USD",
                "USD",
                6,
            )
        return self._token1_data.meta

    def prices(self, timestamps: List[int | datetime]) -> pl.DataFrame:
        timestamps = self._resolve_timetamps(timestamps)
        timestamps = sorted(timestamps)
        updates = self.updates[["timestamp", "price"]].to_dicts()
        i = 0
        price_list = []
        if len(updates) > 0:
            while timestamps[i] < updates[0]["timestamp"]:
                price_list.append(self.initial_price)
                i += 1
        j = 0
        for ts in timestamps[i:]:
            while j < len(updates) and updates[j]["timestamp"] <= ts:
                j += 1
            # now ts < updates[j]
            idx = max([j - 1, 0])
            price_list.append(updates[idx]["price"])
        out = [
            {
                "timestamp": ts,
                "date": datetime.fromtimestamp(ts),
                "price": price,
            }
            for ts, price in zip(timestamps, price_list)
        ]
        return pl.DataFrame(out, ChainlinkUSDData.PRICE_SCHEMA)

    def _build_updates(self) -> pl.DataFrame:
        if self._token0_data is None:
            return self._token1_data.updates

        if self._token1_data is None:
            df = self._token0_data.updates.clone()
            df = df.with_column((1 / pl.col("price")).alias("price"))
            return df

        i = 0
        j = 0
        t0_data = self._token0_data.updates
        t1_data = self._token1_data.updates
        out = []
        last0_price = self._token0_data.initial_price
        last1_price = self._token1_data.initial_price
        while i < len(t0_data) and j < len(t1_data):
            t0_item = t0_data[i].to_dicts()[0]
            t1_item = t1_data[j].to_dicts()[0]
            if t0_item["block_number"] < t1_item["block_number"]:
                i += 1
                last0_price = t0_item["price"]
                t0_item["price"] = last1_price / t0_item["price"]
                out.append(t0_item)
            else:
                j += 1
                last1_price = t1_item["price"]
                t1_item["price"] = t1_item["price"] / last0_price
                out.append(t1_item)

        return pl.DataFrame(out, ChainlinkUSDData.UPDATE_SCHEMA)

    def _resolve_timetamps(self, timestamps: List[int | datetime]) -> List[int]:
        resolved = []
        for ts in timestamps:
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                resolved.append(int(time.mktime(ts.timetuple())))
            else:
                resolved.append(ts)
        return resolved

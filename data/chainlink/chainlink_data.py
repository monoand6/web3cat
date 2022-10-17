from __future__ import annotations
import json
import os
import time
from typing import Any, Dict, List
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

RESOLVER_MAPPING = {"WETH": "ETH", "WBTC": "BTC"}


class ChainlinkUSDData:
    """
    Chainlink data for a specific token.

    When the instance of the class is created, no data is
    fetched. The class has lazy properties like :attr:`updates`
    that are fetched only when accessed.

    See :mod:`data.chainlink` for examples.
    """

    _from_block: int | None
    _to_block: int | None
    _from_date: datetime | None
    _to_date: datetime | None

    _token: str
    _erc20_metas_service: ERC20MetasService
    _events_service: EventsService
    _blocks_service: BlocksService
    _calls_service: CallsService
    _w3: Web3
    _grid_step: int
    _chain_id: int | None

    _meta: ERC20Meta | None
    _oracle_decimals: int | None
    _updates: pl.DataFrame | None
    _oracle_proxy_contract: Contract | None
    _oracle_aggregator_contract: Contract | None
    _index: Dict[str, Any] | None

    def __init__(
        self,
        w3: Web3,
        erc20_metas_service: ERC20MetasService,
        events_service: EventsService,
        blocks_service: BlocksService,
        calls_service: CallsService,
        token: str,
        start: int | datetime,
        end: int | datetime,
        grid_step: int,
    ):
        self._w3 = w3
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
        self._grid_step = grid_step

        self._meta = None
        self._updates = None
        self._oracle_decimals = None
        self._oracle_proxy_contract = None
        self._oracle_aggregator_contract = None
        self._chain_id = None
        self._index = None

    @staticmethod
    def create(
        token: str,
        start: int | datetime,
        end: int | datetime,
        grid_step: int = DEFAULT_BLOCK_TIMESTAMP_GRID,
        cache_path: str = "cache.sqlite3",
        rpc: str | None = None,
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
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
        events_service = EventsService.create(cache_path)
        blocks_service = BlocksService.create(cache_path, rpc)
        calls_service = CallsService.create(cache_path)
        erc20_metas_service = ERC20MetasService.create(cache_path, rpc)

        return ChainlinkUSDData(
            w3=w3,
            erc20_metas_service=erc20_metas_service,
            events_service=events_service,
            blocks_service=blocks_service,
            calls_service=calls_service,
            token=token,
            start=start,
            end=end,
            grid_step=grid_step,
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
            self._from_block = self._blocks_service.get_block_right_after_timestamp(
                ts
            ).number
        return self._from_block

    @property
    def to_block(self):
        """
        End block for the data.
        """
        if not hasattr(self, "_to_block"):
            ts = time.mktime(self._to_date.timetuple())
            self._to_block = self._blocks_service.get_block_right_after_timestamp(
                ts
            ).number
        return self._to_block

    @property
    def chain_id(self) -> int:
        """
        Ethereum chain_id
        """
        if self._chain_id is None:
            self._chain_id = self._w3.eth.chain_id
        return self._chain_id

    @property
    def oracle_proxy_contract(self) -> Contract:
        if self._oracle_proxy_contract is None:
            current_folder = os.path.realpath(os.path.dirname(__file__))
            with open(f"{current_folder}/oracle_proxy.abi.json", "r") as f:
                abi = json.load(f)
            oracle_address = self._resolve_chainlink_address(self.meta.symbol.lower())
            print(oracle_address)
            if oracle_address is None:
                raise LookupError(
                    f"Chainlink oracle for token `{self.meta.symbol.lower()}` on chain with id `{self.chain_id}` is not found"
                )

            self._oracle_proxy_contract = self._w3.eth.contract(
                address=self._w3.toChecksumAddress(oracle_address), abi=abi
            )
        return self._oracle_proxy_contract

    @property
    def oracle_aggregator_contract(self) -> Contract:
        current_folder = os.path.realpath(os.path.dirname(__file__))
        if self._oracle_aggregator_contract is None:
            with open(f"{current_folder}/oracle_aggregator.abi.json", "r") as f:
                abi = json.load(f)
            oracle_address = self._calls_service.get_call(
                self.chain_id,
                self.oracle_proxy_contract.functions.aggregator(),
                self.to_block,
            ).response
            self._oracle_aggregator_contract = self._w3.eth.contract(
                address=self._w3.toChecksumAddress(oracle_address), abi=abi
            )
        return self._oracle_aggregator_contract

    @property
    def oracle_decimals(self) -> int:
        if self._oracle_decimals is None:
            self._oracle_decimals = int(
                self._calls_service.get_call(
                    self.chain_id,
                    self.oracle_proxy_contract.functions.decimals(),
                    self.to_block,
                ).response
            )
        return self._oracle_decimals

    @property
    def updates(self) -> pl.DataFrame:
        if not self._updates:
            self._updates = self._build_updates()
        return self._updates

    def _build_updates(self) -> pl.DataFrame:
        events: List[Event] = self._events_service.get_events(
            self.chain_id,
            self.oracle_aggregator_contract.events.AnswerUpdated,
            self.from_block,
            self.to_block,
        )

        block_numbers = [e.block_number for e in events]
        timestamps = self._blocks_service.get_block_timestamps(
            block_numbers, self._grid_step
        )
        ts_index = {
            block_number: timestamp
            for block_number, timestamp in zip(block_numbers, timestamps)
        }
        factor = 10**self.oracle_decimals
        return pl.from_dicts(
            [self._event_to_row(e, ts_index[e.block_number], factor) for e in events]
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
        cid = str(self.chain_id)
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

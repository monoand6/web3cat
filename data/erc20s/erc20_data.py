from __future__ import annotations
import json
import os
from typing import Any, Dict
from fetcher.blocks.service import DEFAULT_BLOCK_TIMESTAMP_GRID
from fetcher.erc20_metas import ERC20MetasService
from fetcher.erc20_metas import erc20_meta
from fetcher.events import EventsService, Event
from fetcher.blocks import BlocksService
import polars as pl
from web3.contract import Contract
from web3 import Web3
from datetime import datetime
import time
from web3.auto import w3 as w3auto


from fetcher.erc20_metas.erc20_meta import ERC20Meta
from fetcher.db import connection_from_path


class ERC20DataFrame:
    transfers: pl.DataFrame

    def __init__(
        self,
        w3: Web3,
        events_service: EventsService,
        blocks_service: BlocksService,
        meta: ERC20Meta,
        from_block: int,
        to_block: int,
        grid_step: int,
    ):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        erc20_abi = None
        with open(f"{current_folder}/erc20_abi.json", "r") as f:
            erc20_abi = json.load(f)
        chain_id = w3.eth.chain_id
        token: Contract = w3.eth.contract(
            address=w3.toChecksumAddress(meta.address), abi=erc20_abi
        )
        events = events_service.get_events(
            chain_id, token.events.Transfer, from_block, to_block
        )
        block_numbers = [e.block_number for e in events]
        timestamps = blocks_service.get_block_timestamps(block_numbers, grid_step)
        ts_index = {
            block_number: timestamp
            for block_number, timestamp in zip(block_numbers, timestamps)
        }
        factor = 10**meta.decimals
        self.transfers = pl.from_dicts(
            [self._event_to_row(e, ts_index[e.block_number], factor) for e in events]
        )

    def _event_to_row(self, e: Event, ts: int, val_factor: int) -> Dict[str, Any]:
        fr, to, val = list(e.args.values())
        return {
            "timestamp": ts,
            "date": datetime.fromtimestamp(ts),
            "block_number": e.block_number,
            "transaction_hash": e.transaction_hash,
            "log_index": e.log_index,
            "from": fr,
            "to": to,
            "amount": val / val_factor,
        }


class ERC20Data:
    _from_block: int
    _to_block: int
    _token: str
    _erc20_metas_service: ERC20MetasService
    _events_service: EventsService
    _w3: Web3
    _grid_step: int
    _chain_id: int

    _meta: ERC20Meta | None
    _transfers: pl.DataFrame | None

    def __init__(
        self,
        w3: Web3,
        erc20_metas_service: ERC20MetasService,
        events_service: EventsService,
        blocks_service: BlocksService,
        chain_id: int,
        token: str,
        from_block: int,
        to_block: int,
        grid_step: int,
    ):
        self._w3 = w3
        self._token = token
        self._from_block = from_block
        self._to_block = to_block
        self._erc20_metas_service = erc20_metas_service
        self._events_service = events_service
        self._blocks_service = blocks_service
        self._chain_id = chain_id
        self._grid_step = grid_step

        self._meta = None
        self._transfers = None

    @staticmethod
    def create(
        token: str,
        start: int | datetime,
        end: int | datetime,
        grid_step: int = DEFAULT_BLOCK_TIMESTAMP_GRID,
        cache_path: str = "cache.sqlite3",
        rpc: str | None = None,
    ) -> ERC20Data:
        """
        Create an instance of :class:`ERC20Data`

        Args:
            token: Token symbol or address
            start: start of the erc20 data - block number or datetime (inclusive)
            end: end of the erc20 data - block number or datetime (non-inclusive)
            cache_path: path for the cache database
            rpc: Ethereum rpc url. If ``None``, `Web3 auto detection <https://web3py.savethedocs.io/en/stable/providers.html#how-automated-detection-works>`_ is used

        Returns:
            An instance of :class:`BlocksService`
        """
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
        chain_id = w3.eth.chain_id
        events_service = EventsService.create(cache_path)
        blocks_service = BlocksService.create(cache_path, rpc)
        erc20_metas_service = ERC20MetasService.create(cache_path, rpc)
        if type(start) is datetime:
            start = time.mktime(start.timetuple())
            start = blocks_service.get_block_right_after_timestamp(start).number
        if type(end) is datetime:
            end = time.mktime(end.timetuple())
            end = blocks_service.get_block_right_after_timestamp(end - 1).number - 1

        return ERC20Data(
            w3=w3,
            erc20_metas_service=erc20_metas_service,
            events_service=events_service,
            blocks_service=blocks_service,
            chain_id=chain_id,
            token=token,
            from_block=start,
            to_block=end,
            grid_step=grid_step,
        )

    @property
    def meta(self) -> ERC20Meta:
        if not self._meta:
            self._meta = self._erc20_metas_service.get(self._token)
        return self._meta

    @property
    def transfers(self) -> pl.DataFrame:
        if not self._transfers:
            self._build_transfers()
        return self._transfers

    def _build_transfers(self):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        erc20_abi = None
        with open(f"{current_folder}/erc20_abi.json", "r") as f:
            erc20_abi = json.load(f)
        chain_id = self._w3.eth.chain_id
        token: Contract = self._w3.eth.contract(
            address=self._w3.toChecksumAddress(self.meta.address), abi=erc20_abi
        )
        events = self._events_service.get_events(
            chain_id, token.events.Transfer, self._from_block, self._to_block
        )
        block_numbers = [e.block_number for e in events]
        timestamps = self._blocks_service.get_block_timestamps(
            block_numbers, self._grid_step
        )
        ts_index = {
            block_number: timestamp
            for block_number, timestamp in zip(block_numbers, timestamps)
        }
        factor = 10**self.meta.decimals
        self._transfers = pl.from_dicts(
            [self._event_to_row(e, ts_index[e.block_number], factor) for e in events]
        )

    def _event_to_row(self, e: Event, ts: int, val_factor: int) -> Dict[str, Any]:
        fr, to, val = list(e.args.values())
        return {
            "timestamp": ts,
            "date": datetime.fromtimestamp(ts),
            "block_number": e.block_number,
            "transaction_hash": e.transaction_hash,
            "log_index": e.log_index,
            "from": fr,
            "to": to,
            "amount": val / val_factor,
        }

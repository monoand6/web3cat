import json
import os
from typing import Any, Dict
from fetcher.erc20_metas import ERC20MetasService
from fetcher.events import EventsService, Event
from fetcher.blocks import BlocksService
import polars as pl
from web3.contract import Contract
from web3 import Web3
from datetime import datetime

from fetcher.erc20_metas.erc20_meta import ERC20Meta


class ERC20DataFrame:
    transfers: pl.DataFrame

    def __init__(
        self,
        w3: Web3,
        events_service: EventsService,
        blocks_service: BlocksService,
        address: str,
        from_block: int,
        to_block: int,
        grid_step: int,
    ):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        erc20_abi = None
        with open(f"{current_folder}/erc20_abi.json", "r") as f:
            erc20_abi = json.load(f)
        chain_id = w3.eth.chain_id
        token: Contract = w3.eth.contract(address=address, abi=erc20_abi)
        events = events_service.get_events(
            chain_id, token.events.Transfer, from_block, to_block
        )
        block_numbers = [e.block_number for e in events]
        timestamps = blocks_service.get_block_timestamps(block_numbers, grid_step)
        ts_index = {
            block_number: timestamp
            for block_number, timestamp in zip(block_numbers, timestamps)
        }
        self.transfers = pl.from_dicts(
            [self._event_to_row(e, ts_index[e.block_number]) for e in events]
        )

    def _event_to_row(self, e: Event, ts: int) -> Dict[str, Any]:
        return {
            "timestamp": ts,
            "date": datetime.fromtimestamp(ts),
            "block_number": e.block_number,
            "transaction_hash": e.transaction_hash,
            "log_index": e.log_index,
            "from": e.args["from"],
            "to": e.args["to"],
            "amount": e.args["amount"],
        }


class ERC20Data:
    _from_block: int
    _to_block: int
    _zero_balances: bool
    _token: str
    _erc20_metas_service: ERC20MetasService
    _events_service: EventsService
    _w3: Web3
    _grid_step: int
    _chain_id: int

    _meta: ERC20Meta | None
    _data: ERC20DataFrame | None

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
        zero_initial_balances: bool = False,
    ):
        self._w3 = w3
        self._token = token
        self._zero_balances = zero_initial_balances
        self._from_block = from_block
        self._to_block = to_block
        self._erc20_metas_service = erc20_metas_service
        self._events_service = events_service
        self._blocks_service = blocks_service
        self._chain_id = chain_id
        self._grid_step = grid_step

        self._meta = None
        self._data = None

    @property
    def meta(self) -> ERC20Meta:
        if not self._meta:
            self._meta = self._erc20_metas_service.get(self._token)
        return self._meta

    @property
    def data(self) -> ERC20DataFrame:
        if not self._data:
            self._data = ERC20DataFrame(
                self._w3,
                self._events_service,
                self._blocks_service,
                self.meta.address,
                self._from_block,
                self._to_block,
                self._grid_step,
            )
        return self._data

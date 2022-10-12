from __future__ import annotations
from bisect import bisect
import json
import os
import numpy as np
from typing import Any, Dict, List, Tuple
from fetcher.blocks.service import DEFAULT_BLOCK_TIMESTAMP_GRID
from fetcher.erc20_metas import ERC20MetasService
from fetcher.erc20_metas import erc20_meta
from fetcher.events import EventsService, Event
from fetcher.blocks import BlocksService
from fetcher.calls import CallsService
import polars as pl
from web3.contract import Contract
from web3 import Web3
from datetime import datetime
import time
from web3.auto import w3 as w3auto


from fetcher.erc20_metas.erc20_meta import ERC20Meta
from fetcher.db import connection_from_path


class ERC20Data:
    """
    Data for a specific ERC20 token.

    See :mod:`data.erc20s` for examples and details.
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
    _address_filter: List[str]
    _chain_id: int | None

    _meta: ERC20Meta | None
    _transfers: pl.DataFrame | None
    _token_contract: Contract | None

    def __init__(
        self,
        w3: Web3,
        erc20_metas_service: ERC20MetasService,
        events_service: EventsService,
        blocks_service: BlocksService,
        calls_service: CallsService,
        token: str,
        address_filter: List[str] | None,
        start: int | datetime,
        end: int | datetime,
        grid_step: int,
    ):
        self._w3 = w3
        self._token = token
        self._address_filter = address_filter or []
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
        self._transfers = None
        self._token_contract = None
        self._chain_id = None

    @staticmethod
    def create(
        token: str,
        start: int | datetime,
        end: int | datetime,
        address_filter: List[str] | None = None,
        grid_step: int = DEFAULT_BLOCK_TIMESTAMP_GRID,
        cache_path: str = "cache.sqlite3",
        rpc: str | None = None,
    ) -> ERC20Data:
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
        events_service = EventsService.create(cache_path)
        blocks_service = BlocksService.create(cache_path, rpc)
        calls_service = CallsService.create(cache_path)
        erc20_metas_service = ERC20MetasService.create(cache_path, rpc)

        return ERC20Data(
            w3=w3,
            erc20_metas_service=erc20_metas_service,
            events_service=events_service,
            blocks_service=blocks_service,
            calls_service=calls_service,
            token=token,
            address_filter=address_filter,
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
    def transfers(self) -> pl.DataFrame:
        """
        Dataframe with transfers data
        (see `polars.Dataframe <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html>`_)

        **Fields**

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp of the transfer event                                              |
        |                      |                            | (approximate, see :meth:`fetcher.blocks.BlocksService.get_block_timestamps`  |
        |                      |                            | for details)                                                                 |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :attr:`numpy.int64`        | Block number for this transfer                                               |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``transaction_hash`` | :class:`str`               | Transaction hash for this transfer                                           |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``log_index``        | :attr:`numpy.int64`        | Log index inside the transaction for this transfer                           |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``from``             | :class:`str`               | The address from which erc20 token was sent                                  |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``to``               | :class:`str`               | The address to which erc20 token was sent                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``value``            | :attr:`numpy.float64`      | Transfer value in natural token units (e.g. eth for weth, not wei)           |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        """
        if self._transfers is None:
            self._build_transfers()
        return self._transfers

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
    def volumes(self) -> pl.DataFrame:
        """
        Dataframe with transfer volumes and balance changes by address
        (see `polars.Dataframe <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html>`_).

        **Fields**

        +-------------+-----------------------+----------------------------------------------+
        | Name        | Type                  | Description                                  |
        +=============+=======================+==============================================+
        | ``address`` | :class:`str`          | Address                                      |
        +-------------+-----------------------+----------------------------------------------+
        | ``volume``  | :attr:`numpy.float64` | Volume in natural token units                |
        +-------------+-----------------------+----------------------------------------------+
        | ``change``  | :attr:`numpy.float64` | Change for the period in natural token units |
        +-------------+-----------------------+----------------------------------------------+
        """
        df1 = (
            self.transfers[["from", "value"]]
            .groupby("from")
            .agg(pl.col("value").sum())
            .rename({"from": "address", "value": "volume"})
            .with_column((pl.col("volume") * (-1)).alias("change"))
        )
        df2 = (
            self.transfers[["to", "value"]]
            .groupby("to")
            .agg(pl.col("value").sum())
            .rename({"to": "address", "value": "volume"})
            .with_column(pl.col("volume").alias("change"))
        )
        return (
            df1.extend(df2)
            .groupby("address")
            .agg([pl.col("volume").sum(), pl.col("change").sum()])
            .sort(pl.col("volume"), reverse=True)
        )

    @property
    def chain_id(self) -> int:
        """
        Ethereum chain_id
        """
        if self._chain_id is None:
            self._chain_id = self._w3.eth.chain_id
        return self._chain_id

    @property
    def token_contract(self) -> Contract:
        """
        :class:`web3.contracts.Contract` for this token
        """
        current_folder = os.path.realpath(os.path.dirname(__file__))
        if self._token_contract is None:
            with open(f"{current_folder}/erc20_abi.json", "r") as f:
                erc20_abi = json.load(f)
            self._token_contract = self._w3.eth.contract(
                address=self._w3.toChecksumAddress(self.meta.address), abi=erc20_abi
            )
        return self._token_contract

    def balances(
        self,
        address: str,
        timestamps: List[int | datetime],
        initial_balance: int | None = None,
    ) -> pl.DataFrame:
        """
        Get `polars.Dataframe <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html>`_ with
        balances over time for a specific address.

        Args:
            address: The address for balances
            timestamps: A series of timestamps or datetimes to fetch balances for. Not that timestamps are not exact, see :meth:`fetcher.blocks.BlocksService.get_block_timestamps` for details
            initial_balance: Initial balance of the address (at the :class:`ERC20Data` start). If ``None``, the balance is fetched from the blockchain.

        Returns a Dataframe with fields

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp of the transfer event                                              |
        |                      |                            | (approximate, see :meth:`fetcher.blocks.BlocksService.get_block_timestamps`  |
        |                      |                            | for details)                                                                 |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``balance``          | :attr:`numpy.float64`      | Balance in natural token units (e.g. eth for weth, not wei)                  |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        if initial_balance is None:
            first_block = self.transfers["block_number"][0]
            initial_balance_wei = self._calls_service.get_call(
                self.chain_id,
                self.token_contract.functions.balanceOf(
                    Web3.toChecksumAddress(address)
                ),
                first_block - 1,
            ).response
            initial_balance = initial_balance_wei / 10**self.meta.decimals

        for i in range(len(timestamps)):
            ts = timestamps[i]
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                timestamps[i] = int(time.mktime(ts.timetuple()))
        timestamps = sorted(timestamps)

        bs = self._zero_balances(address, timestamps)
        res = [
            {
                "timestamp": ts,
                "date": datetime.fromtimestamp(ts),
                "balance": balance + initial_balance,
            }
            for ts, balance in zip(timestamps, bs)
        ]
        return pl.DataFrame(res)

    def _zero_balances(
        self, address: str, timestamps: List[int | datetime]
    ) -> List[np.float64]:
        if len(timestamps) == 0:
            return []

        address = address.lower()
        transfers = (
            self.transfers.filter(
                (pl.col("from") == address) | (pl.col("to") == address)
            )
            .filter((pl.col("from") != address) | (pl.col("to") != address))
            .with_column(
                pl.when(pl.col("from") == address)
                .then(pl.col("value") * (-1))
                .otherwise(pl.col("value"))
                .alias("cash_flow")
            )[["timestamp", "cash_flow"]]
            .to_dicts()
        )
        j = 0
        balance = 0
        res = []
        for ts in timestamps:
            while j < len(transfers) and transfers[j]["timestamp"] <= ts:
                balance += transfers[j]["cash_flow"]
                j += 1
            res.append(balance)
        return res

    def _build_transfers(self):
        events = []
        for filters in self._build_argument_filters():
            fetched_events = self._events_service.get_events(
                self.chain_id,
                self.token_contract.events.Transfer,
                self.from_block,
                self.to_block,
                argument_filters=filters,
            )
            events += fetched_events

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
        ).sort(pl.col("timestamp"))

    def _build_argument_filters(
        self,
    ) -> List[Dict[str, Any] | None, Dict[str, Any] | None]:
        if len(self._address_filter) == 0:
            return [None]
        return [{"from": self._address_filter}, {"to": self._address_filter}]

    def _event_to_row(self, e: Event, ts: int, val_factor: int) -> Dict[str, Any]:
        fr, to, val = list(e.args.values())
        return {
            "timestamp": ts,
            "date": datetime.fromtimestamp(ts),
            "block_number": e.block_number,
            "transaction_hash": e.transaction_hash,
            "log_index": e.log_index,
            "from": fr.lower(),
            "to": to.lower(),
            "value": val / val_factor,
        }

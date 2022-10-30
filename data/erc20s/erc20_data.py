# pylint: disable=line-too-long

from __future__ import annotations
from functools import cached_property
from typing import Any, Dict, List
from datetime import datetime
import time

import numpy as np
import polars as pl
from web3.contract import Contract
from web3.constants import ADDRESS_ZERO
from web3 import Web3
from data.core import DataCore

from fetcher.erc20_metas import ERC20MetasService
from fetcher.events import EventsService, Event
from fetcher.blocks import BlocksService
from fetcher.calls import CallsService
from fetcher.erc20_metas.erc20_meta import ERC20Meta


class ERC20Data(DataCore):
    """
    Datasets for a specific ERC20 token.
    """

    _start: int | datetime
    _end: int | datetime

    _token: str
    _address_filter: List[str]
    _erc20_metas_service: ERC20MetasService
    _events_service: EventsService
    _blocks_service: BlocksService
    _calls_service: CallsService

    _meta: ERC20Meta | None
    _transfers: pl.DataFrame | None
    _mints_burns: pl.DataFrame | None
    _token_contract: Contract | None

    def __init__(
        self,
        token: str,
        address_filter: List[str] | None,
        start: int | datetime,
        end: int | datetime,
        **kwargs,
    ):
        super().__init__(start, end, **kwargs)  # pylint: disable=too-many-function-args
        self._token = token
        self._address_filter = address_filter or []

    @cached_property
    def meta(self) -> ERC20Meta:
        """
        Metadata for tokens (like name, symbol and decimals)
        """
        return self._erc20_metas_service.get(self._token)

    @cached_property
    def transfers(self) -> pl.DataFrame:
        """
        Dataframe with transfers for addresses specified by the ``address_filter ``.

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
        return self._fetch_transfers(self._address_filter)

    @cached_property
    def emission(self) -> pl.DataFrame:
        """
        All mints and burns.

        **Schema**

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
        | ``from``             | :class:`str`               | If non-zero: the burn address                                                |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``to``               | :class:`str`               | If non-zero: the mint address                                                |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``value``            | :attr:`numpy.float64`      | Mint / Burn value in natural token units (e.g. eth for weth, not wei)        |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        """

        return self._fetch_transfers([ADDRESS_ZERO])

    @property
    def volumes(self) -> pl.DataFrame:
        """
        Dataframe with transfer volumes and balance changes by address.

        **Schema**

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

    def total_supply(
        self,
        timepoints: List[int | datetime],
    ):
        """
        Dataframe with total supply of the token

        **Schema**

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp of the transfer event                                              |
        |                      |                            | (approximate, see :meth:`fetcher.blocks.BlocksService.get_block_timestamps`  |
        |                      |                            | for details)                                                                 |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``total_supply``     | :attr:`numpy.float64`      | Total supply in natural tokens (e.g. eth for weth, not wei)                  |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        initial_balance_wei = self._calls_service.get_call(
            self.meta.contract.functions.totalSupply(),
            self.from_block_number - 1,
        ).response
        initial_total_supply = initial_balance_wei / 10**self.meta.decimals
        timestamps = sorted(self._resolve_timepoints(timepoints, to_blocks=False))
        bs = self._accrued_balances(ADDRESS_ZERO, timestamps, self.mints_burns)
        out = [
            {
                "timestamp": ts,
                "date": datetime.fromtimestamp(ts),
                "total_supply": initial_total_supply - balance,
            }
            for ts, balance in zip(timestamps, bs)
        ]
        return pl.DataFrame(
            out,
            {"timestamp": pl.UInt64, "date": pl.Datetime, "total_supply": pl.Float64},
        )

    def balance(
        self,
        addresses: List[str],
        timestamps: List[int | datetime],
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
        | ``block_number``     | :attr:`numpy.int64`        | Block number                                                                 |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``address``          | :class:`str`               | Ethereum Address                                                             |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``balance``          | :attr:`numpy.float64`      | Balance in natural token units (e.g. eth for weth, not wei)                  |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        addresses = sorted([a.lower() for a in addresses])
        if not self._address_filter is None:
            mismatched = set(addresses).difference(set(self._address_filter))
            if len(mismatched) > 0:
                raise ValueError(
                    f"Addresses {', '.join(mismatched)} are not in address_filter for this set. "
                    "Please add them when initializing ERC20Data."
                )
        initial_balances = []
        for address in addresses:
            initial_balance_wei = self._calls_service.get_call(
                self.token_contract.functions.balanceOf(
                    Web3.toChecksumAddress(address)
                ),
                first_block - 1,
            ).response
        if initial_balance is None:
            if len(self.transfers) > 0:
                first_block = self.transfers["block_number"][0]
            else:
                first_block = self._blocks_service.get_latest_blocks_by_timestamps(
                    timestamps[0]
                )[0].number
            initial_balance_wei = self._calls_service.get_call(
                self.token_contract.functions.balanceOf(
                    Web3.toChecksumAddress(address)
                ),
                first_block - 1,
            ).response
            initial_balance = initial_balance_wei / 10**self.meta.decimals
        timestamps = sorted(self._resolve_timestamps(timestamps))

        bs = self._accrued_balances(address, timestamps, self.transfers)
        out = [
            {
                "timestamp": ts,
                "date": datetime.fromtimestamp(ts),
                "balance": balance + initial_balance,
            }
            for ts, balance in zip(timestamps, bs)
        ]
        return pl.DataFrame(
            out, {"timestamp": pl.UInt64, "date": pl.Datetime, "balance": pl.Float64}
        )

    def balance(
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
            if len(self.transfers) > 0:
                first_block = self.transfers["block_number"][0]
            else:
                first_block = self._blocks_service.get_latest_blocks_by_timestamps(
                    timestamps[0]
                )[0].number
            initial_balance_wei = self._calls_service.get_call(
                self.token_contract.functions.balanceOf(
                    Web3.toChecksumAddress(address)
                ),
                first_block - 1,
            ).response
            initial_balance = initial_balance_wei / 10**self.meta.decimals
        timestamps = sorted(self._resolve_timestamps(timestamps))

        bs = self._accrued_balances(address, timestamps, self.transfers)
        out = [
            {
                "timestamp": ts,
                "date": datetime.fromtimestamp(ts),
                "balance": balance + initial_balance,
            }
            for ts, balance in zip(timestamps, bs)
        ]
        return pl.DataFrame(
            out, {"timestamp": pl.UInt64, "date": pl.Datetime, "balance": pl.Float64}
        )

    def balance_for_addresses(
        self, addresses: List[str], timestamps: List[int | datetime]
    ):
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

        out = []
        for addr in addresses:
            balances = self.balances(addr, timestamps).to_dicts()
            balances = [
                {
                    "timestamp": b["timestamp"],
                    "date": b["date"],
                    "address": addr.lower(),
                    "balance": b["balance"],
                }
                for b in balances
            ]
            out += balances

        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "address": pl.Utf8,
                "balance": pl.Float64,
            },
        ).sort(pl.col("timestamp"))

    def _resolve_timestamps(self, timestamps: List[int | datetime]) -> List[int]:
        resolved = []
        for ts in timestamps:
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                resolved.append(int(time.mktime(ts.timetuple())))
            else:
                resolved.append(ts)
        return resolved

    def _accrued_balances(
        self, address: str, timestamps: List[int], events: pl.DataFrame
    ) -> List[np.float64]:
        if len(timestamps) == 0:
            return []

        address = address.lower()
        transfers = (
            events.filter((pl.col("from") == address) | (pl.col("to") == address))
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
        out = []
        for ts in timestamps:
            while j < len(transfers) and transfers[j]["timestamp"] <= ts:
                balance += transfers[j]["cash_flow"]
                j += 1
            out.append(balance)
        return out

    def _build_transfers(self, address_filter: List[str]) -> pl.DataFrame:
        events: List[Event] = []
        for filters in self._build_argument_filters(address_filter):
            fetched_events = self._events_service.get_events(
                self.token_contract.events.Transfer,
                self.from_block,
                self.to_block,
                argument_filters=filters,
            )
            events += fetched_events

        block_numbers = [e.block_number for e in events]
        blocks = self._blocks_service.get_blocks(block_numbers)
        timestamps = [b.timestamp for b in blocks]
        ts_index = {
            block_number: timestamp
            for block_number, timestamp in zip(block_numbers, timestamps)
        }
        factor = 10**self.meta.decimals
        transfers = pl.DataFrame(
            [self._event_to_row(e, ts_index[e.block_number], factor) for e in events],
            ERC20Data.TRANSFER_SCHEMA,
        )
        return transfers.unique(subset=["transaction_hash", "log_index"]).sort(
            pl.col("timestamp")
        )

    def _fetch_transfers(self, addresses: List[str]) -> pl.DataFrame:
        events: List[Event] = []

        # Fetch with "from" and "to" filter
        for filters in self._build_argument_filters(addresses):
            fetched_events = self._events_service.get_events(
                self.meta.contract.events.Transfer,
                self.from_block,
                self.to_block,
                argument_filters=filters,
            )
            events += fetched_events

        block_numbers = list(set(e.block_number for e in events))
        blocks = self._blocks_service.get_blocks(block_numbers)
        ts_index = {b.number: b.timestamp for b in blocks}
        factor = 10**self.meta.decimals
        transfers = pl.DataFrame(
            [self._event_to_row(e, ts_index[e.block_number], factor) for e in events],
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "transaction_hash": pl.Utf8,
                "log_index": pl.UInt64,
                "from": pl.Utf8,
                "to": pl.Utf8,
                "value": pl.Float64,
            },
        )
        return transfers.unique(subset=["transaction_hash", "log_index"]).sort(
            pl.col("timestamp")
        )

    def _build_argument_filters(
        self, address_filter: List[str]
    ) -> List[Dict[str, Any] | None, Dict[str, Any] | None]:
        if len(address_filter) == 0:
            return [None]
        return [{"from": address_filter}, {"to": address_filter}]

    def _event_to_row(self, event: Event, ts: int, val_factor: int) -> Dict[str, Any]:
        fr, to, val = list(event.args.values())
        return {
            "timestamp": ts,
            "date": datetime.fromtimestamp(ts),
            "block_number": event.block_number,
            "transaction_hash": event.transaction_hash,
            "log_index": event.log_index,
            "from": fr.lower(),
            "to": to.lower(),
            "value": val / val_factor,
        }

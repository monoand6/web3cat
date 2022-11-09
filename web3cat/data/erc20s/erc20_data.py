from __future__ import annotations
from functools import cached_property
from typing import Any, Dict, List
from datetime import datetime

import numpy as np
import polars as pl
from web3.contract import Contract
from web3.constants import ADDRESS_ZERO
from web3 import Web3
from web3cat.data.core import DataCore

from web3cat.fetcher.events import Event
from web3cat.fetcher.erc20_metas.erc20_meta import ERC20Meta


class ERC20Data(DataCore):
    """
    Datasets for ERC20 token.

    Args:
        token: Token name or address
        address_filter: Limit token transfer data only to these addresses.
                        All transfers for mainstream tokens is a big chunk of data.
                        This optimization makes fetches faster. This filter
                        doesn't apply to mints, burns, and total_supply.
        start: Starting timepoint
        end: Ending timepoint
    """

    _start: int | datetime
    _end: int | datetime

    _token: str
    _address_filter: List[str]

    _meta: ERC20Meta | None
    _transfers: pl.DataFrame | None
    _emission: pl.DataFrame | None
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
        self._address_filter = [addr.lower() for addr in (address_filter or [])]

    @property
    def contract(self) -> Contract:
        """
        A :class:`web3.contract.Contract` for this token
        """
        return self.meta.contract

    @cached_property
    def meta(self) -> ERC20Meta:
        """
        Metadata for tokens (like name, symbol and decimals)
        """
        return self._erc20_metas_service.get(self._token)

    @property
    def address_filter(self) -> List[str]:
        """
        Address filter for this data (transfers only for these addreses)
        """
        return self._address_filter

    @cached_property
    def transfers(self) -> pl.DataFrame:
        """
        Dataframe with transfers for addresses specified by the ``address_filter``.

        **Schema**

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp of the transfer event                                              |
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
        | ``value``            | :attr:`numpy.float64`      | Transfer value                                                               |
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
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :attr:`numpy.int64`        | Block number for this mint / burn                                            |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``transaction_hash`` | :class:`str`               | Transaction hash for this mint / burn                                        |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``log_index``        | :attr:`numpy.int64`        | Log index inside the transaction for this mint / burn                        |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``from``             | :class:`str`               | If non-zero: the burn address                                                |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``to``               | :class:`str`               | If non-zero: the mint address                                                |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``value``            | :attr:`numpy.float64`      | Transfer value                                                               |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        """

        if self.meta.symbol.upper() == "USDT":
            return self._fetch_usdt_emission()
        return self._fetch_transfers([ADDRESS_ZERO])

    @property
    def volume(self) -> pl.DataFrame:
        """
        Dataframe with transfer volumes and balance changes by address.

        **Schema**

        +-------------+-----------------------+----------------------------------------------+
        | Name        | Type                  | Description                                  |
        +=============+=======================+==============================================+
        | ``address`` | :class:`str`          | Address                                      |
        +-------------+-----------------------+----------------------------------------------+
        | ``volume``  | :attr:`numpy.float64` | Volume for the period                        |
        +-------------+-----------------------+----------------------------------------------+
        | ``change``  | :attr:`numpy.float64` | Change for the period (aka net volume)       |
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
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp of total supply snapshot                                           |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :attr:`numpy.int64`        | Block number for this total supply snapshot                                  |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``total_supply``     | :attr:`numpy.float64`      | Total supply in natural tokens (e.g. eth for weth, not wei)                  |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        initial_balance_wei = self._calls_service.get_call(
            self.contract.functions.totalSupply(),
            self.from_block_number - 1,
        ).response
        initial_total_supply = initial_balance_wei / 10**self.meta.decimals
        blocks = sorted(self._resolve_timepoints(timepoints), key=lambda x: x.number)
        block_numbers = [b.number for b in blocks]
        bs = self._accrued_balances(ADDRESS_ZERO, block_numbers, self.emission)
        out = [
            {
                "timestamp": b.timestamp,
                "date": datetime.fromtimestamp(b.timestamp),
                "block_number": b.number,
                "total_supply": initial_total_supply - balance,
            }
            for b, balance in zip(blocks, bs)
        ]
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "total_supply": pl.Float64,
            },
        )

    def balances(
        self,
        addresses: List[str],
        timepoints: List[int | datetime],
    ) -> pl.DataFrame:
        """
        Dataframe with balances for addresses over time.

        Args:
            addresses: The list of addresses
            timepoints: A list of timepoints

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
        | ``address``          | :class:`str`               | Ethereum Address                                                             |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``balance``          | :attr:`numpy.float64`      | Balance of an address at the time                                            |
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
        initial_balance_calls = [
            self.contract.functions.balanceOf(Web3.toChecksumAddress(addr))
            for addr in addresses
        ]
        initial_balances = self._calls_service.get_calls(
            initial_balance_calls, [self.from_block_number]
        )
        blocks = sorted(self._resolve_timepoints(timepoints), key=lambda x: x.number)
        block_numbers = [b.number for b in blocks]
        out = []
        factor = 10**self.meta.decimals
        for i, addr in enumerate(addresses):
            initial_balance = initial_balances[i].response / factor
            bs = self._accrued_balances(addr, block_numbers, self.transfers)
            out += [
                {
                    "timestamp": b.timestamp,
                    "date": datetime.fromtimestamp(b.timestamp),
                    "block_number": b.number,
                    "address": addr,
                    "balance": balance + initial_balance,
                }
                for b, balance in zip(blocks, bs)
            ]
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "address": pl.Utf8,
                "balance": pl.Float64,
            },
        ).sort("block_number")

    def _accrued_balances(
        self, address: str, blocks: List[int], events: pl.DataFrame
    ) -> List[np.float64]:
        if len(blocks) == 0:
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
            )[["block_number", "timestamp", "cash_flow"]]
            .to_dicts()
        )
        j = 0
        balance = 0
        out = []
        for b in blocks:
            while j < len(transfers) and transfers[j]["block_number"] <= b:
                balance += transfers[j]["cash_flow"]
                j += 1
            out.append(balance)
        return out

    def _fetch_usdt_emission(self) -> pl.DataFrame:
        events = []
        owner = self._calls_service.get_call(
            self.contract.functions.owner(), self.from_block_number
        ).response.lower()
        for raw_event in self._events_service.get_events(
            self.contract.events.Issue,
            self.from_block_number,
            self.to_block_number,
        ):
            raw_event.args = {
                "from": ADDRESS_ZERO,
                "to": owner,
                "value": raw_event.args["amount"],
            }
            events.append(raw_event)

        for raw_event in self._events_service.get_events(
            self.contract.events.Redeem,
            self.from_block_number,
            self.to_block_number,
        ):
            raw_event.args = {
                "from": owner,
                "to": ADDRESS_ZERO,
                "value": raw_event.args["amount"],
            }
            events.append(raw_event)
        return self._transform_raw_events(events)

    def _fetch_transfers(self, addresses: List[str]) -> pl.DataFrame:
        events: List[Event] = []

        # Fetch with "from" and "to" filter
        for filters in self._build_argument_filters(addresses):
            fetched_events = self._events_service.get_events(
                self.contract.events.Transfer,
                self.from_block_number,
                self.to_block_number,
                argument_filters=filters,
            )
            events += fetched_events

        return self._transform_raw_events(events)

    def _transform_raw_events(self, events: List[Event]) -> pl.DataFrame:
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

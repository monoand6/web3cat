from __future__ import annotations
from typing import List
from datetime import datetime
import polars as pl

from data.core import DataCore


class EtherData(DataCore):
    """
    Data for ETH balances
    """

    def balances(
        self, addresses: List[str], timepoints: List[int | datetime]
    ) -> pl.DataFrame:
        """
        Get ether balances.

        Arguments:
            addresses: a list of addresses to fetch balances for
            timepoints: a list of timepoints

        Returns:
            A Dataframe with fields

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp for the snapshot of the balance                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :class:`int`               | Number of the block                                                          |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``address``          | :class:`str`               | Address for the balance                                                      |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``balance``          | :attr:`numpy.float64`      | Balance of an address at the time                                            |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        blocks = self._resolve_timepoints(timepoints)
        blocks_idx = {b.number: b for b in blocks}
        block_numbers = [b.number for b in blocks]
        balances = self._balances_service.get_balances(addresses, block_numbers)
        out = [
            {
                "timestamp": blocks_idx[bal.block_number].timestamp,
                "date": datetime.fromtimestamp(blocks_idx[bal.block_number].timestamp),
                "block_number": blocks_idx[bal.block_number].number,
                "address": bal.address,
                "balance": bal.balance,
            }
            for bal in balances
        ]
        df = pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "address": pl.Utf8,
                "balance": pl.Float64,
            },
        ).sort(["timestamp", "address"])
        return df

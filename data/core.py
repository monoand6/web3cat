"""
Base classes for data classes
"""

from datetime import datetime
from functools import cached_property
import time
from typing import List
from web3 import Web3

from fetcher.balances import BalancesService
from fetcher.blocks import BlocksService
from fetcher.blocks.block import Block
from fetcher.calls import CallsService
from fetcher.erc20_metas import ERC20MetasService
from fetcher.events import EventsService


ETH_START_TIMESTAMP = int(time.mktime(datetime(2015, 7, 30).timetuple()))


class DataCore:
    """
    Base class for all data classes
    """

    start: int | datetime
    end: int | datetime
    _balances_service: BalancesService
    _blocks_service: BlocksService
    _calls_service: CallsService
    _erc20_metas_service: ERC20MetasService
    _events_service: EventsService

    def __init__(self, start: int | datetime, end: int | datetime, **kwargs):
        self.start = start
        self.end = end
        service_args = {
            k: kwargs.pop(k)
            for k in [
                "balances_service",
                "blocks_service",
                "calls_service",
                "erc20_metas_service",
                "events_service",
            ]
            if k in kwargs
        }

        if "balances_service" in service_args:
            self._balances_service = service_args["balances_service"]
        else:
            self._balances_service = BalancesService.create(**kwargs)

        if "blocks_service" in service_args:
            self._blocks_service = service_args["blocks_service"]
        else:
            self._blocks_service = BlocksService.create(**kwargs)

        if "calls_service" in service_args:
            self._calls_service = service_args["calls_service"]
        else:
            self._calls_service = CallsService.create(**kwargs)

        if "erc20_metas_service" in service_args:
            self._erc20_metas_service = service_args["erc20_metas_service"]
        else:
            self._erc20_metas_service = ERC20MetasService.create(**kwargs)

        if "events_service" in service_args:
            self._events_service = service_args["events_service"]
        else:
            self._events_service = EventsService.create(**kwargs)

    @cached_property
    def from_block_number(self) -> int:
        """
        Start block number for the data.
        """
        return self._resolve_timepoints([self.start])[0].number

    @cached_property
    def to_block_number(self) -> int:
        """
        End block number for the data.
        """
        return self._resolve_timepoints([self.end])[0].number

    @cached_property
    def from_timestamp(self) -> int:
        """
        Start unix timestamp for the data.
        """

        return self._resolve_timepoints([self.start])[0].timestamp

    @cached_property
    def to_timestamp(self) -> int:
        """
        End unix timestamp for the data.
        """

        return self._resolve_timepoints([self.end])[0].timestamp

    @property
    def from_date(self) -> datetime:
        """
        Start datetime for the data.
        """

        return datetime.fromtimestamp(self.from_timestamp)

    @property
    def to_date(self) -> datetime:
        """
        End datetime for the data.
        """

        return datetime.fromtimestamp(self.to_timestamp)

    @cached_property
    def w3(self) -> Web3:
        """
        An instance of :class:`web3.Web3`
        """
        return self._blocks_service.w3  # pylint: disable=protected-access

    def _resolve_timepoints(self, timepoints: List[int | datetime]) -> List[Block]:
        resolved_dates = []
        for t in timepoints:
            if isinstance(t, datetime):
                t = int(time.mktime(t.timetuple()))
            resolved_dates.append(t)
        timestamps = []
        blocks = []
        for ts in resolved_dates:
            # This works because on each chain block_time > 1s
            # It means that timepoint is a block
            if ts < ETH_START_TIMESTAMP:
                blocks.append(ts)
            else:
                timestamps.append(ts)
        timestamps_idx = {}
        blocks_idx = {}

        resolved = self._blocks_service.get_latest_blocks_by_timestamps(timestamps)
        i = 0
        for ts in timestamps:
            timestamps_idx[ts] = resolved[i]
            i += 1

        resolved = self._blocks_service.get_blocks(blocks)
        i = 0
        for b in blocks:
            blocks_idx[b] = resolved[i]
            i += 1

        return [blocks_idx.get(tp, timestamps_idx.get(tp)) for tp in resolved_dates]

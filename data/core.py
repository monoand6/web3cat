from datetime import datetime
from functools import cached_property
from time import time
from typing import List
from fetcher.balances import BalancesService
from fetcher.blocks import BlocksService
from fetcher.calls import CallsService
from fetcher.erc20_metas import ERC20MetasService
from fetcher.events import EventsService


ETH_START_TIMESTAMP = int(time.mktime(datetime(2015, 7, 30).timetuple()))


class DataCore:
    """
    Base class for all data classes
    """

    _start: int | datetime
    _end: int | datetime
    _balances_service: BalancesService
    _blocks_service: BlocksService
    _calls_service: CallsService
    _erc20_metas_service: ERC20MetasService
    _events_service: EventsService

    def __init__(self, start: int | datetime, end: int | datetime, **kwargs):
        self._start = start
        self._end = end
        self._balances_service = BalancesService.create(**kwargs)
        self._blocks_service = BlocksService.create(**kwargs)
        self._calls_service = CallsService.create(**kwargs)
        self._erc20_metas_service = ERC20MetasService.create(**kwargs)
        self._events_service = EventsService.create(**kwargs)

    @cached_property
    def from_block_number(self) -> int:
        """
        Start block number for the data.
        """
        return self._resolve_timepoints(self._start, to_blocks=True)

    @cached_property
    def to_block_number(self) -> int:
        """
        End block number for the data.
        """
        return self._resolve_timepoints(self._end, to_blocks=True)

    @cached_property
    def from_timestamp(self) -> int:
        """
        Start unix timestamp for the data.
        """

        return self._resolve_timepoints(self._start, to_blocks=False)

    @cached_property
    def to_timestamp(self) -> int:
        """
        End unix timestamp for the data.
        """

        return self._resolve_timepoints(self._end, to_blocks=False)

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

    def _resolve_timepoints(self, timepoints: List[int | datetime], to_blocks: bool):
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
        if to_blocks:
            resolved = self._blocks_service.get_latest_blocks_by_timestamps(timestamps)
            i = 0
            for ts in timestamps:
                timestamps_idx[ts] = resolved[i].number
                i += 1
            blocks_idx = {b: b for b in blocks}
        else:
            resolved = self._blocks_service.get_blocks(blocks)
            i = 0
            for b in blocks:
                blocks_idx[b] = resolved[i].timestamp
                i += 1
            timestamps_idx = {ts: ts for ts in timestamps}

        return [blocks_idx.get(tp, timestamps_idx.get(tp)) for tp in resolved_dates]

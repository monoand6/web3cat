from datetime import datetime
from functools import cached_property
from time import time
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
        return self._resolve_block(self._start, self._blocks_service)

    @cached_property
    def to_block_number(self) -> int:
        """
        End block number for the data.
        """
        return self._resolve_block(self._end, self._blocks_service)

    @cached_property
    def from_timestamp(self) -> int:
        """
        Start unix timestamp for the data.
        """

        block = self._blocks_service.get_blocks(self.from_block_number)[0]
        return block.timestamp

    @cached_property
    def to_timestamp(self) -> int:
        """
        End unix timestamp for the data.
        """

        block = self._blocks_service.get_blocks(self.to_block_number)[0]
        return block.timestamp

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

    def _resolve_block(self, timepoint: int | datetime, blocks_service: BlocksService):
        """
        Resolve a block.

        Args:
            timepoint: block or Unix timestamp or datetime

        Returns:
            Block
        """

        if isinstance(timepoint, datetime):
            timepoint = int(time.mktime(timepoint.timetuple()))

        # This works because on each chain block_time > 1s
        # It means that timepoint is a block
        if timepoint < ETH_START_TIMESTAMP:
            return timepoint

        return blocks_service.get_latest_block_at_timestamp(timepoint)

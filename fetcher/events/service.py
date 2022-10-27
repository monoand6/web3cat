from __future__ import annotations
import json
from typing import Any, Dict, List, Tuple
from web3.contract import ContractEvent
from requests.exceptions import ReadTimeout

from fetcher.core import Core
from fetcher.events.event import Event
from fetcher.events.repo import EventsRepo
from fetcher.events_indices.index import EventsIndex
from fetcher.events_indices.index_data import EventsIndexData
from fetcher.events_indices.repo import EventsIndicesRepo
from fetcher.utils import json_response, print_progress, short_address


class EventsService(Core):
    """
    Service for fetching contract events.

    This service fetches contract events from web3, cache them,
    and read from the cache on subsequent calls.

    **Request/Response flow**

    ::

                   +---------------+              +-------+ +-------------------+ +-------------+
                   | EventsService |              | Web3  | | EventsIndicesRepo | | EventsRepo  |
                   +---------------+              +-------+ +-------------------+ +-------------+
        -----------------  |                          |               |                  |
        | Request events |-|                          |               |                  |
        |----------------| |                          |               |                  |
                           |                          |               |                  |
                           | Request index            |               |                  |
                           |----------------------------------------->|                  |
                           |                          |               |                  |
                           | Fetch missing events     |               |                  |
                           |------------------------->|               |                  |
                           |                          |               |                  |
                           | Store missing events     |               |                  |
                           |------------------------------------------------------------>|
                           |                          |               |                  |
                           | Fetch all events         |               |                  |
                           |------------------------------------------------------------>|
          ---------------  |                          |               |                  |
          | Response     |-|                          |               |                  |
          |--------------| |                          |               |                  |
                           |                          |               |                  |

    Args:
        events_repo: Repo of events
        events_indices_repo: Repo of events_indices
        kwargs: Args for the :class:`fetcher.core.Core`
    """

    _events_repo: EventsRepo
    _events_indices_repo: EventsIndicesRepo

    def __init__(
        self, events_repo: EventsRepo, events_indices_repo: EventsIndicesRepo, **kwargs
    ):
        super().__init__(**kwargs)
        self._events_repo = events_repo
        self._events_indices_repo = events_indices_repo

    @staticmethod
    def create(**kwargs) -> EventsService:
        """
        Create an instance of :class:`EventsService`

        Args:
            kwargs: Args for the :class:`fetcher.core.Core`

        Returns:
            An instance of :class:`EventsService`
        """
        events_repo = EventsRepo(**kwargs)
        events_indices_repo = EventsIndicesRepo(**kwargs)
        return EventsService(events_repo, events_indices_repo)

    def get_events(
        self,
        event: ContractEvent,
        from_block: int,
        to_block: int,
        argument_filters: Dict[str, Any] | None = None,
    ) -> List[Event]:
        """
        Get events specified by parameters.

        Args:
            event: class:`web3.contract.ContractEvent` specifying contract and event_name.
            from_block: fetch events from this block (inclusive)
            to_block: fetch events from this block (non-inclusive)
            argument_filters: Additional filters for events search.
                              Example: :code:`{"from": "0xfa45..."}`

        Returns:
            A list of fetched events

        Exceptions:
            See :meth:`prefetch_events`
        """
        self.prefetch_events(event, from_block, to_block, argument_filters)
        all_events = self._events_repo.find(
            event.event_name,
            event.address,
            from_block=from_block,
            to_block=to_block,
            argument_filters=argument_filters,
        )
        return list(all_events)

    def prefetch_events(
        self,
        event: ContractEvent,
        from_block: int,
        to_block: int,
        argument_filters: Dict[str, Any] | None = None,
    ):
        """
        Fetch events specified by parameters and save them to cache.

        Args:
            event: class:`web3.contract.ContractEvent` specifying contract and event_name.
            from_block: fetch events from this block (inclusive)
            to_block: fetch events from this block (non-inclusive)
            argument_filters: Additional filters for events search.
                              Example: :code:`{"from": "0xfa45..."}`

        Exceptions:
            This method tries to fetch all events from :code:`from_block`
            to :code:`to_block` at once. More often than not, rpc endpoint
            will block such attempts and ask to use a narrower block interval
            for fetch. In this case, the interval is halved, and fetch is retried.
            This is repeated until success.

            However, if at some point the interval is less than
            :const:`fetcher.events_indices.constants.BLOCKS_PER_BIT`,
            :class:`RuntimeError` is raised.
        """
        read_indices = list(
            self._events_indices_repo.find_indices(
                event.address, event.event_name, argument_filters
            )
        )
        write_index = self._events_indices_repo.get_index(
            event.address, event.event_name, argument_filters
        )
        if write_index is None:
            write_index = EventsIndex(
                self.chain_id,
                event.address,
                event.event_name,
                argument_filters,
                EventsIndexData(),
            )
        current_chunk_size_in_steps = (to_block - from_block) // write_index.step() + 1
        fetched = False
        while not fetched:
            if current_chunk_size_in_steps == 0:
                raise RuntimeError(
                    "Couldn't fetch data because minimum chunk size is reached"
                )
            try:
                self._fetch_events_for_chunk_size(
                    current_chunk_size_in_steps,
                    event,
                    argument_filters,
                    from_block,
                    to_block,
                    read_indices,
                    write_index,
                )
                fetched = True
            except (ValueError, ReadTimeout):
                current_chunk_size_in_steps //= 2

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._events_indices_repo.purge()
        self._events_repo.purge()
        self._events_indices_repo.conn.commit()
        self._events_repo.conn.commit()

    def _fetch_events_for_chunk_size(
        self,
        chunk_size_in_steps: int,
        event: ContractEvent,
        argument_filters: Dict[str, Any] | None,
        from_block: int,
        to_block: int,
        read_indices: List[EventsIndex],
        write_index: EventsIndex,
    ):
        from_block = write_index.data.snap_block_to_grid(from_block)
        offset = 0 if to_block == write_index.data.snap_block_to_grid(to_block) else 1
        to_block = (
            write_index.data.snap_block_to_grid(to_block) + offset * write_index.step()
        )
        prefix = (
            f"Fetching {event.event_name}@{short_address(event.address)} "
            f"({from_block} - {to_block})"
        )
        step = chunk_size_in_steps * write_index.step()
        for start in range(from_block, to_block, step):
            end = min(start + step, to_block)
            shinked_start, shrinked_end = self._shrink_blocks(read_indices, start, end)
            if shinked_start < shrinked_end:
                print_progress(
                    start - from_block,
                    to_block - from_block,
                    prefix=prefix,
                )
                self._fetch_and_save_events_in_one_chunk(
                    event,
                    argument_filters,
                    shinked_start,
                    shrinked_end,
                    write_index,
                )
            print_progress(
                end - from_block,
                to_block - from_block,
                prefix=prefix,
            )

    def _shrink_blocks(
        self, read_indices: List[EventsIndex], from_block: int, to_block: int
    ) -> Tuple[int, int]:
        """
        Shorten a block range so that both start and end are facing 0 bits.

        Example:
          from            to            from  to
            |             |               |   |
            111111100101111   =>   111111100101111
        """
        if len(read_indices) == 0:
            return (from_block, to_block)
        step = read_indices[0].step()
        shrinked_start, shrinked_end = from_block, to_block
        while (
            self._block_is_in_indices(read_indices, shrinked_start)
            and shrinked_start <= shrinked_end
        ):
            shrinked_start += step
        while (
            self._block_is_in_indices(read_indices, shrinked_end - step)
            and shrinked_start <= shrinked_end
        ):
            shrinked_end -= step
        return (shrinked_start, shrinked_end)

    def _block_is_in_indices(self, indices: List[EventsIndex], block: int) -> bool:
        if block < 0:
            return False
        for i in indices:
            if i.data[block]:
                return True
        return False

    def _fetch_and_save_events_in_one_chunk(
        self,
        event: ContractEvent,
        argument_filters: Dict[str, Any] | None,
        from_block: int,
        to_block: int,
        write_index: EventsIndex,
    ) -> List[Event]:
        events = self._fetch_events_in_one_chunk(
            event, from_block, to_block, argument_filters
        )
        self._events_repo.save(events)
        write_index.data.set_range(from_block, to_block, True)
        self._events_indices_repo.save([write_index])
        self._events_indices_repo.conn.commit()

    def _fetch_events_in_one_chunk(
        self,
        event: ContractEvent,
        from_block: int,
        to_block: int,
        argument_filters: Dict[str, Any] | None,
    ) -> List[Event]:
        event_filter = event.createFilter(
            fromBlock=from_block,
            toBlock=to_block - 1,
            argument_filters=argument_filters,
        )
        entries = event_filter.get_all_entries()
        jsons = [json.loads(json_response(e)) for e in entries]
        events = [
            Event(
                chain_id=self.chain_id,
                block_number=j["blockNumber"],
                transaction_hash=j["transactionHash"],
                log_index=j["logIndex"],
                address=j["address"],
                event=j["event"],
                args=j["args"],
            )
            for j in jsons
        ]
        return events

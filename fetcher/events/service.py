from __future__ import annotations
import sys
import json
from typing import Any, Dict, List, Tuple
from fetcher.db import connection_from_path
from fetcher.events.event import Event
from fetcher.events.repo import EventsRepo
from fetcher.events_indices.index import EventsIndex
from fetcher.events_indices.index_data import EventsIndexData
from fetcher.events_indices.repo import EventsIndicesRepo
from web3 import Web3
from web3.contract import ContractEvent
from web3.auto import w3 as w3auto

from fetcher.utils import json_response, short_address


class EventsService:
    """
    Service for fetching web3 events.

    The sole purpose of this service is to fetch events from web3, cache them,
    and read from the cache on subsequent calls.

    The exact flow goes like this
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

    """

    _events_repo: EventsRepo
    _events_indices_repo: EventsIndicesRepo
    _last_progress_bar_length: int

    def __init__(self, events_repo: EventsRepo, events_indices_repo: EventsIndicesRepo):
        self._events_repo = events_repo
        self._events_indices_repo = events_indices_repo
        self._last_progress_bar_length = 0

    @staticmethod
    def create(cache_path: str = "cache.sqlite3") -> EventsService:
        """
        Create an instance of :class:`EventsService`

        Args:
            cache_path: path for the cache database

        Returns:
            An instance of :class:`EventsService`
        """
        conn = connection_from_path(cache_path)
        events_repo = EventsRepo(conn)
        events_indices_repo = EventsIndicesRepo(conn)
        return EventsService(events_repo, events_indices_repo)

    def get_events(
        self,
        chain_id: int,
        event: ContractEvent,
        from_block: int,
        to_block: int,
        argument_filters: Dict[str, Any] | None = None,
    ) -> List[Event]:
        """
        Get events specified by parameters.

        Args:
            chain_id: Ethereum chain_id
            event: class:`web3.contract.ContractEvent` specifying contract and event_name.
            from_block: fetch events from this block (inclusive)
            to_block: fetch events from this block (non-inclusive)
            argument_filters: Additional filters for events search. Example: :code:`{"from": "0xfa45..."}`

        Returns:
            A list of fetched events

        Exceptions:
            See :meth:`prefetch_events`
        """
        self.prefetch_events(chain_id, event, from_block, to_block, argument_filters)
        all_events = self._events_repo.find(
            chain_id, event.event_name, event.address, from_block, to_block
        )
        return [e for e in all_events if e.matches_filter(argument_filters)]

    def prefetch_events(
        self,
        chain_id: int,
        event: ContractEvent,
        from_block: int,
        to_block: int,
        argument_filters: Dict[str, Any] | None = None,
    ):
        """
        Fetch events specified by parameters and save them to cache.

        Args:
            chain_id: Ethereum chain_id
            event: class:`web3.contract.ContractEvent` specifying contract and event_name.
            from_block: fetch events from this block (inclusive)
            to_block: fetch events from this block (non-inclusive)
            argument_filters: Additional filters for events search. Example: :code:`{"from": "0xfa45..."}`

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
        read_indices = self._events_indices_repo.find_indices(
            chain_id, event.address, event.event_name, argument_filters
        )
        write_index = self._events_indices_repo.get_index(
            chain_id, event.address, event.event_name, argument_filters
        )
        if write_index is None:
            write_index = EventsIndex(
                chain_id,
                event.address,
                event.event_name,
                argument_filters,
                EventsIndexData(),
            )
        current_chunk_size_in_steps = (to_block - from_block) // write_index.step() + 1
        fetched = False
        e_memoized = None
        while not fetched:
            if current_chunk_size_in_steps == 0:
                if not e_memoized:
                    raise RuntimeError(
                        "Couldn't fetch data because minimum chunk size is reached"
                    )
                else:
                    raise e_memoized
            try:
                self._fetch_events_for_chunk_size(
                    current_chunk_size_in_steps,
                    chain_id,
                    event,
                    argument_filters,
                    from_block,
                    to_block,
                    read_indices,
                    write_index,
                )
                fetched = True
            # ValueError is for error of exceeding log size
            # However requests.exceptions.ReadTimeout also happens sometimes, so it's better to use catch-all
            except Exception as e:
                e_memoized = e
                current_chunk_size_in_steps //= 2

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._events_indices_repo.purge()
        self._events_repo.purge()
        self._events_indices_repo.commit()
        self._events_repo.commit()

    def _fetch_events_for_chunk_size(
        self,
        chunk_size_in_steps: int,
        chain_id: int,
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
        prefix = f"Fetching {event.event_name}@{short_address(event.address)} ({from_block} - {to_block})"
        step = chunk_size_in_steps * write_index.step()
        should_print_progress = False
        for start in range(from_block, to_block, step):
            end = min(start + step, to_block)
            shinked_start, shrinked_end = self._shrink_blocks(read_indices, start, end)
            if shinked_start < shrinked_end:
                should_print_progress = True
                self._print_progress(
                    start - from_block, to_block - from_block, prefix=prefix
                )
                self._fetch_and_save_events_in_one_chunk(
                    chain_id,
                    event,
                    argument_filters,
                    shinked_start,
                    shrinked_end,
                    write_index,
                )
            if should_print_progress:
                self._print_progress(
                    end - from_block, to_block - from_block, prefix=prefix
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
        chain_id: int,
        event: ContractEvent,
        argument_filters: Dict[str, Any] | None,
        from_block: int,
        to_block: int,
        write_index: EventsIndex,
    ) -> List[Event]:
        events = self._fetch_events_in_one_chunk(
            chain_id, event, from_block, to_block, argument_filters
        )
        self._events_repo.save(events)
        write_index.data.set_range(from_block, to_block, True)
        self._events_indices_repo.save([write_index])
        self._events_indices_repo.commit()

    def _fetch_events_in_one_chunk(
        self,
        chain_id: int,
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
                chain_id=chain_id,
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

    def _print_progress(
        self, iteration, total, prefix="", suffix="", decimals=1, bar_length=20
    ):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            bar_length  - Optional  : character length of bar (Int)
        """
        str_format = "{0:." + str(decimals) + "f}"
        percents = str_format.format(100 * (iteration / float(total)))
        filled_length = int(round(bar_length * iteration / float(total)))
        bar = "█" * filled_length + "-" * (bar_length - filled_length)
        text = "%s |%s| %s%s %s\r" % (prefix, bar, percents, "%", suffix)
        sys.stdout.write("%s\r" % (" " * self._last_progress_bar_length)),
        sys.stdout.write(text),
        self._last_progress_bar_length = len(text)

        if iteration == total:
            sys.stdout.write("\n")
            sys.stdout.flush()

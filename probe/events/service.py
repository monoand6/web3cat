from __future__ import annotations
import sys
import json
from typing import Any, Dict, List
from probe.events.event import Event
from probe.events.repo import EventsRepo
from probe.events_indices.index import EventsIndex
from probe.events_indices.index_data import EventsIndexData
from probe.events_indices.repo import EventsIndicesRepo
from web3 import Web3
from web3.contract import ContractEvent
from web3.auto import w3 as w3auto

from probe.w3_utils import json_response, short_address
from probe.db import DB


class EventsService:
    _events_repo: EventsRepo
    _events_indices_repo: EventsIndicesRepo
    _w3: Web3

    def __init__(
        self, events_repo: EventsRepo, events_indices_repo: EventsIndicesRepo, w3: Web3
    ):
        self._events_repo = events_repo
        self._events_indices_repo = events_indices_repo
        self._w3 = w3

    def create(
        cache_path: str = "cache.sqlite3", rpc: str | None = None
    ) -> EventsService:
        db = DB.from_path(cache_path)
        events_repo = EventsRepo(db)
        events_indices_repo = EventsIndicesRepo(db)
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
        return EventsService(events_repo, events_indices_repo, w3)

    def get_events(
        self,
        chain_id: int,
        event: ContractEvent,
        from_block: int,
        to_block: int,
        argument_filters: Dict[str, Any] | None = None,
    ):
        self._fetch_events(chain_id, event, argument_filters, from_block, to_block)
        return self._events_repo.find(
            chain_id, event.event_name, event.address, from_block, to_block
        )

    def _fetch_events(
        self,
        chain_id: int,
        event: ContractEvent,
        argument_filters: Dict[str, Any] | None,
        from_block: int,
        to_block: int,
    ):
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
        current_chunk_size = (to_block - from_block) // write_index.step() + 1
        fetched = False
        e_memoized = None
        while not fetched:
            if current_chunk_size == 0:
                if not e_memoized:
                    raise RuntimeError(
                        "Couldn't fetch data because chunk size = 0 is reached"
                    )
                else:
                    raise e_memoized
            try:
                self._fetch_events_for_chunk_size(
                    current_chunk_size,
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
                current_chunk_size //= 2

    def _fetch_events_for_chunk_size(
        self,
        chunk_size: int,
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

        chunks = ((to_block - from_block) // write_index.step()) // chunk_size
        current_block = from_block
        for c in range(chunks):
            self._print_progress(
                c,
                chunks,
                prefix=f"Fetching `{event.event_name}` event for `{short_address(event.address)}`",
                suffix=f"{chunk_size * write_index.step()} events per fetch",
            )
            from_block_local = current_block
            to_block_local = current_block + chunk_size * write_index.step()
            if self._is_in_indices(read_indices, from_block, to_block):
                continue
            self._fetch_and_save_events_in_one_chunk(
                chain_id,
                event,
                argument_filters,
                from_block_local,
                to_block_local,
                write_index,
            )
            current_block += chunk_size * write_index.step()
        if current_block < to_block:
            self._print_progress(
                chunks,
                chunks,
                prefix=f"Fetching `{event.event_name}` event for `{short_address(event.address)}`",
                suffix=f"{chunk_size * write_index.step()} events per fetch",
            )
            if not self._is_in_indices(read_indices, current_block, to_block):
                self._fetch_and_save_events_in_one_chunk(
                    chain_id,
                    event,
                    argument_filters,
                    from_block_local,
                    to_block_local,
                    write_index,
                )

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

    def _is_in_indices(
        self, indices: List[EventsIndex], start_block: int, end_block: int
    ) -> bool:
        for index in indices:
            if self._is_in_index(index, start_block, end_block):
                return True
        return False

    def _is_in_index(
        self, index: EventsIndex, start_block: int, end_block: int
    ) -> bool:
        if end_block == 0:
            return True
        if not index.data[end_block]:
            return False
        for b in range(start_block, end_block, index.step()):
            if not index.data[b]:
                return False
        return True

    def _pick_index(
        event_indices: List[EventsIndex],
        chain_id: str,
        event: ContractEvent,
        args: Dict[str, Any] | None,
        start_block: int,
        end_block: int,
    ) -> EventsIndex:
        if len(event_indices) == 0:
            return EventsIndex(
                chain_id=chain_id,
                address=event.address,
                event=event.event_name,
                args=args,
                data=EventsIndexData(),
            )

    def _print_progress(
        self, iteration, total, prefix="", suffix="", decimals=1, bar_length=10
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

        sys.stdout.write("\r%s |%s| %s%s %s" % (prefix, bar, percents, "%", suffix)),

        if iteration == total:
            sys.stdout.write("\n")
        sys.stdout.flush()


# class EventsDB:
#     _db: DB

#     def __init__(self, db: DB):
#         self._db = db

#     def fetch_events(
#         self,
#         chain_id: int,
#         event: ContractEvent,
#         argument_filters: Dict[str, Any] | None,
#         start_timestamp: int,
#         end_timestamp: int,
#     ) -> List[Event] | None:
#         indexes = self._find_index_matches(chain_id, event, argument_filters)
#         is_indexed = False
#         for ind in indexes:
#             if self._is_fetched(ind, start_timestamp, end_timestamp):
#                 is_indexed = True
#                 break
#         if not is_indexed:
#             return None

#         cur = self._db.cursor()

#         # to be updated
#         cur.execute(
#             "SELECT * FROM events WHERE event = ? AND address = ? AND blockNumber >= ? AND blockNumber < ? AND chainId = ?",
#             (event, address.lower(), from_block, to_block, chain_id),
#         )
#         rows = cur.fetchall()
#         events = []
#         filters = argument_filters or dict()
#         for x in rows:
#             e = _parse_event(x)
#             skip = False
#             for k, v in filters.items():
#                 if skip:
#                     break
#                 val = e["args"][k]
#                 if isinstance(val, str):
#                     val = val.lower()
#                 filter_vals = v if isinstance(v, list) else [v]
#                 skip = True
#                 for filter_val in filter_vals:
#                     if isinstance(filter_val, str):
#                         filter_val = filter_val.lower()
#                     if val == filter_val:
#                         skip = False
#                         break
#             if skip:
#                 continue
#             events.append(e)

#         return events

#     def _is_fetched(
#         self, index: EventsIndexData, start_timestamp: int, end_timestamp: int
#     ) -> bool:
#         if not index[end_timestamp]:
#             return False
#         for ts in range(start_timestamp, end_timestamp, SECONDS_IN_BIT):
#             if not index[ts]:
#                 return False
#         return True

#     def _find_index_matches(
#         self,
#         chain_id: int,
#         event: ContractEvent,
#         argument_filters: Dict[str, Any] | None,
#     ) -> List[EventsIndexData]:
#         cur = self._db.cursor()

#         cur.execute(
#             "SELECT * FROM events_indices WHERE chain_id = ? AND address = ? AND name = ?",
#             (chain_id, event.address, event.name),
#         )
#         rows = cur.fetchall()
#         # sort by length of the args
#         rows = sorted(rows, key=lambda r: args_len(r[3]), reverse=True)
#         args = dump_args(argument_filters)
#         rows = [r for r in rows if args_match(r, args)]
#         return [EventsIndexData.load(r) for r in rows]


# def args_len(j: str | None) -> int:
#     if j is None:
#         return 0
#     return len(json.loads(j).keys())


# def dump_args(argument_filters: Dict[str, Any] | None) -> str | None:
#     if argument_filters is None or len(argument_filters.keys()) == 0:
#         return None
#     res = {}
#     for k in sorted(argument_filters.keys()):
#         v = argument_filters[k]
#         if not isinstance(v, list):
#             v = [v]
#         v = [val.lower() if isinstance(val, str) else val for val in sorted(v)]
#         res[k] = v
#     return json.dumps(res)


# def _event_index_key(
#     chain_id: int, event: ContractEvent, argument_filters: Dict[str, Any] | None
# ) -> str:
#     index_key = f"{chain_id}|{event.address.lower()}|{event.event_name}"
#     if argument_filters:
#         # sort keys by name and filter values by values
#         f = {}
#         for k in sorted(argument_filters.keys()):
#             v = argument_filters[k]
#             if not isinstance(v, list):
#                 v = [v]
#             v = [val.lower() if isinstance(val, str) else val for val in sorted(v)]
#             f[k] = v
#         index_key += f"|{json.dumps(f)}"
#     return index_key


# def _events_cache_key(
#     event: ContractEvent,
#     from_block: int,
#     to_block: int,
#     chain_id: int,
#     argument_filters: Optional[Dict[str, Any]],
# ) -> str:
#     key = f"{event.event_name}_{event.address}_{from_block}_{to_block}_{chain_id}"
#     if argument_filters:
#         f = {}
#         for k in sorted(argument_filters.keys()):
#             v = argument_filters[k]
#             if not isinstance(v, list):
#                 v = [v]
#             v = [val.lower() if isinstance(val, str) else val for val in sorted(v)]
#             f[k] = v
#         key += "_"
#         key += json.dumps(f)

#     return key

# def fetch_events(event: ContractEvent, from_block: int, to_block: int, chain_id: int = 1, argument_filters: Optional[Dict[str, Any]] = None) -> List[Dict]:
#     chunk_size = to_block - from_block
#     last_e = None
#     while chunk_size > 100:
#         try:
#             return _fetch_events_with_chunk_size(event, from_block, to_block, chain_id, chunk_size, argument_filters)
#         # ValueError is for error of exceeding log size
#         # However requests.exceptions.ReadTimeout also happens sometimes, so it's better to use catch-all
#         except Exception as e:
#             logging.info("Fetch resulted in error, decreasing chunk size by half")
#             logging.info(e)
#             chunk_size = int(chunk_size / 2)
#     logging.info(f"Reached chunk_size limit {chunk_size}, quitting")

# def _fetch_events_with_chunk_size(event: ContractEvent, from_block: int, to_block: int, chain_id: int, chunk_size: int, argument_filters: Optional[Dict[str, Any]]):
#     logging.info(f"Fetching event {event.event_name} at address {event.address} from {from_block} to {to_block} for chain {chain_id} with chunk size {chunk_size}")
#     if is_in_events_cache(event, from_block, to_block, chain_id, argument_filters):
#         # was cached before, i.e. guaranteed to be in database
#         # can't rely on actual database record, since if the first existing event starts at block 3
#         # and you query for block 2, there will always be a cache miss in db
#         logging.info("Cache hit: returning db data")
#         return get_db_events(event.event_name, event.address, from_block, to_block, chain_id, argument_filters)

#     chunks = int((to_block - from_block) / chunk_size)
#     logging.info(f"Cache miss: fetching in {chunks} chunks")
#     current_block = from_block
#     for c in range(chunks):
#         logging.info(f"Chunk {c} / {chunks}")
#         _fetch_and_cache_events(event, current_block, current_block + chunk_size, chain_id, argument_filters)
#         current_block += chunk_size
#     if current_block < to_block:
#         logging.info("Last chunk")
#         _fetch_and_cache_events(event, current_block, to_block, chain_id, argument_filters)
#     add_to_events_cache(event, from_block, to_block, chain_id, argument_filters)
#     return get_db_events(event.event_name, event.address, from_block, to_block, chain_id, argument_filters)

# class Web3JsonEncoder(json.JSONEncoder):
#     def default(self, obj: Any) -> Union[Dict[Any, Any], HexStr]:
#         if isinstance(obj, AttributeDict):
#             return {k: v for k, v in obj.items()}
#         if isinstance(obj, HexBytes):
#             return HexStr(obj.hex())
#         if isinstance(obj, (bytes, bytearray)):
#             return HexStr(HexBytes(obj).hex())
#         return json.JSONEncoder.default(self, obj)

# def _fetch_and_cache_events(event: ContractEvent, from_block: int, to_block: int, chain_id: int, argument_filters: Optional[Dict[str, Any]]):
#     logging.info(f"    Fetching event {event.event_name} at address {event.address} from {from_block} to {to_block} for chain {chain_id}")
#     if is_in_events_cache(event, from_block, to_block, chain_id, argument_filters):
#         logging.info("        Cache hit: do nothing")
#         return
#     logging.info("        Cache miss: fetching from web3")
#     event_filter = event.createFilter(fromBlock = from_block, toBlock = to_block - 1, argument_filters = argument_filters)
#     entries = event_filter.get_all_entries()
#     j = json.dumps(entries, cls=Web3JsonEncoder)
#     je = np.array(json.loads(j))
#     for x in je:
#         x["chainId"] = chain_id
#     chunk_size = 100
#     logging.info(f"        Fetched {len(je)} events, saving to db")
#     for chunk in np.array_split(je, len(je) / chunk_size + 1):
#         write_db_events(chunk)
#     add_to_events_cache(event, from_block, to_block, chain_id, argument_filters)

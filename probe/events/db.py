from calendar import leapdays
import json
from operator import sub
from typing import Any, Dict, List
from web3.contract import ContractEvent
from probe.db import DB
from probe.events.index import EventsIndexData, SECONDS_IN_BIT
from probe.events.model import Event


class EventsDB:
    _db: DB

    def __init__(self, db: DB):
        self._db = db

    def fetch_events(
        self,
        chain_id: int,
        event: ContractEvent,
        argument_filters: Dict[str, Any] | None,
        start_timestamp: int,
        end_timestamp: int,
    ) -> List[Event] | None:
        indexes = self._find_index_matches(chain_id, event, argument_filters)
        is_indexed = False
        for ind in indexes:
            if self._is_fetched(ind, start_timestamp, end_timestamp):
                is_indexed = True
                break
        if not is_indexed:
            return None

        cur = self._db.cursor()

        # to be updated
        cur.execute(
            "SELECT * FROM events WHERE event = ? AND address = ? AND blockNumber >= ? AND blockNumber < ? AND chainId = ?",
            (event, address.lower(), from_block, to_block, chain_id),
        )
        rows = cur.fetchall()
        events = []
        filters = argument_filters or dict()
        for x in rows:
            e = _parse_event(x)
            skip = False
            for k, v in filters.items():
                if skip:
                    break
                val = e["args"][k]
                if isinstance(val, str):
                    val = val.lower()
                filter_vals = v if isinstance(v, list) else [v]
                skip = True
                for filter_val in filter_vals:
                    if isinstance(filter_val, str):
                        filter_val = filter_val.lower()
                    if val == filter_val:
                        skip = False
                        break
            if skip:
                continue
            events.append(e)

        return events

    def _is_fetched(
        self, index: EventsIndexData, start_timestamp: int, end_timestamp: int
    ) -> bool:
        if not index[end_timestamp]:
            return False
        for ts in range(start_timestamp, end_timestamp, SECONDS_IN_BIT):
            if not index[ts]:
                return False
        return True

    def _find_index_matches(
        self,
        chain_id: int,
        event: ContractEvent,
        argument_filters: Dict[str, Any] | None,
    ) -> List[EventsIndexData]:
        cur = self._db.cursor()

        cur.execute(
            "SELECT * FROM events_indices WHERE chain_id = ? AND address = ? AND name = ?",
            (chain_id, event.address, event.name),
        )
        rows = cur.fetchall()
        # sort by length of the args
        rows = sorted(rows, key=lambda r: args_len(r[3]), reverse=True)
        args = dump_args(argument_filters)
        rows = [r for r in rows if args_match(r, args)]
        return [EventsIndexData.load(r) for r in rows]


def args_is_subset(subset: Any | None, superset: Any | None) -> bool:
    if subset is None:
        return True
    if superset is None:
        if isinstance(subset, dict) and len(subset.keys()) == 0:
            return True
        return False
    if isinstance(subset, dict):
        if not isinstance(superset, dict):
            return False
        for key in subset.keys():
            if not key in superset:
                return False
            if not args_is_subset(subset[key], superset[key]):
                return False
        return True
    if isinstance(subset, list):
        if not isinstance(superset, list):
            return False
        sb = set(subset)
        sp = set(superset)
        return sb.issubset(sp)
    return subset == superset


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

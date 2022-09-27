from typing import Any, Dict, List

from probe.db import DB
from probe.events_indices.index import EventsIndex


class EventsIndicesRepo:
    _db: DB

    def __init__(self, db: DB):
        self._db = db

    def find_indices(
        self, address: str, event: str, args: Dict[str, Any] | None = None
    ) -> List[EventsIndex]:
        cursor = self._db.cursor()
        cursor.execute(
            f"SELECT * FROM events_indices WHERE address = ? AND event = ?",
            (address, event),
        )
        rows = cursor.fetchall()
        indices = [EventsIndex.from_tuple(r) for r in rows]
        return [i for i in indices if args_is_subset(args, i.args)]

    def save(self, index: EventsIndex):
        cursor = self._db.cursor()
        cursor.execute(
            "INSERT INTO events_indices (chain_id,address,event,args,mask) VALUES (?,?,?,?,?)",
            index.to_tuple(),
        )


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

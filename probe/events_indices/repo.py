from typing import Any, Dict, List

import json
from probe.db import DB
from probe.events_indices.index import EventsIndex
from probe.repo import Repo


class EventsIndicesRepo(Repo):
    def find_indices(
        self,
        chain_id: int,
        address: str,
        event: str,
        args: Dict[str, Any] | None = None,
    ) -> List[EventsIndex]:
        cursor = self._db.cursor()
        cursor.execute(
            f"SELECT * FROM events_indices WHERE chain_id = ? AND address = ? AND event = ?",
            (chain_id, address, event),
        )
        rows = cursor.fetchall()
        indices = [EventsIndex.from_tuple(r) for r in rows]
        return [i for i in indices if args_is_subset(args, i.args)]

    def get_index(
        self,
        chain_id: int,
        address: str,
        event: str,
        args: Dict[str, Any] | None = None,
    ) -> EventsIndex | None:
        args = args or {}
        cursor = self._db.cursor()
        cursor.execute(
            f"SELECT * FROM events_indices WHERE chain_id = ? AND address = ? AND event = ? and args = ?",
            (chain_id, address, event, json.dumps(EventsIndex.normalize_args(args))),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return EventsIndex.from_tuple(row)

    def save(self, indices: List[EventsIndex]):
        cursor = self._db.cursor()
        rows = [i.to_tuple() for i in indices]
        cursor.executemany(
            "INSERT INTO events_indices VALUES (?,?,?,?,?) ON CONFLICT DO UPDATE SET mask = excluded.mask",
            rows,
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

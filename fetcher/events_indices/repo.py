from typing import Any, Dict, List

import json
from fetcher.events_indices.index import EventsIndex
from fetcher.db import Repo


class EventsIndicesRepo(Repo):
    """
    Reading and writing :class:`EventsIndex` to database.
    """

    def find_indices(
        self,
        chain_id: int,
        address: str,
        event: str,
        args: Dict[str, Any] | None = None,
    ) -> List[EventsIndex]:
        """
        Find all indices that match :code:`chain_id`, :code:`address`,
        :code:`event`, and :code:`args`.

        The match for :code:`args` is non-trivial.
        It tries to find all indices that could have the events for
        the specific :code:`args`.

        Example
        ~~~~~~~

        Imagine two indices for the ERC20 Transfer event:

        1. Stating that all :code:`Transfer` events were fetched from block 2000 to 4000
        2. Stating that :code:`Transfer` from address "0x6b17..." events were fetched from block 3000 to 4000

        Now we want to query if the :code:`Transfer` events were fetched for
        :code:`{"from": "0x6b17..."}` for blocks 2500 to 4000. A naive implementation
        would return just the :code:`{"from": "0x6b17..."}` index stating that blocks
        2500 to 3000 are missing. However, these events were already fetched
        as part of all :code:`Transfer` events from block 2000 to 4000.
        That's why a list of indices is returned, so the check is made
        against the data that is really missing.

        Args:
            chain_id: Ethereum chain_id
            address: Contract address
            event: Event name
            args: Argument filters
        """
        cursor = self._connection.cursor()
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
        """
        Find an index with the exact match of :code:`chain_id`, :code:`address`,
        :code:`event`, and :code:`args`.

        Args:
            chain_id: Ethereum chain_id
            address: Contract address
            event: Event name
            args: Argument filters

        """

        args = args or {}
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT * FROM events_indices WHERE chain_id = ? AND address = ? AND event = ? and args = ?",
            (chain_id, address, event, json.dumps(EventsIndex.normalize_args(args))),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return EventsIndex.from_tuple(row)

    def save(self, indices: List[EventsIndex]):
        cursor = self._connection.cursor()
        rows = [i.to_tuple() for i in indices]
        cursor.executemany(
            "INSERT INTO events_indices VALUES (?,?,?,?,?) ON CONFLICT DO UPDATE SET data = excluded.data",
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

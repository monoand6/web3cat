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
        Find all indices that match :code:`chain_id`, ``address``,
        ``event``, and ``args``.

        The match for ``args`` is non-trivial.
        It tries to find all indices that could have the events for
        the specific ``args``.

        Example
        ~~~~~~~

        Imagine two indices for the ERC20 Transfer event:

        1. Stating that all ``Transfer`` events were fetched from block 2000 to 4000
        2. Stating that ``Transfer`` from address "0x6b17..." events were fetched from block 3000 to 4000

        Now we want to query if the ``Transfer`` events were fetched for
        :code:`{"from": "0x6b17..."}` for blocks 2500 to 4000. A naive implementation
        would return just the :code:`{"from": "0x6b17..."}` index stating that blocks
        2500 to 3000 are missing. However, these events were already fetched
        as part of all ``Transfer`` events from block 2000 to 4000.
        That's why a list of indices is returned, so the check is made
        against the data that is really missing.

        Args:
            chain_id: Ethereum chain_id
            address: Contract address
            event: Event name
            args: Argument filters

        Returns:
            List of matched indices
        """
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT * FROM events_indices WHERE chain_id = ? AND address = ? AND event = ?",
            (chain_id, address, event),
        )
        rows = cursor.fetchall()
        indices = [EventsIndex.from_tuple(r) for r in rows]
        return [i for i in indices if is_softer_filter_than(i.args, args)]

    def get_index(
        self,
        chain_id: int,
        address: str,
        event: str,
        args: Dict[str, Any] | None = None,
    ) -> EventsIndex | None:
        """
        Find an index with the exact match of :code:`chain_id`, ``address``,
        ``event``, and ``args``.

        Args:
            chain_id: Ethereum chain_id
            address: Contract address
            event: Event name
            args: Argument filters

        Returns:
            Found index, or ``None`` if nothing is found
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
        """
        Save indices to database.

        Args:
            indices: a list of indices to save
        """
        cursor = self._connection.cursor()
        rows = [i.to_tuple() for i in indices]
        cursor.executemany(
            "INSERT INTO events_indices VALUES (?,?,?,?,?) ON CONFLICT DO UPDATE SET data = excluded.data",
            rows,
        )

    def purge(self):
        """
        Clean all database entries
        """
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM events_indices")


def is_softer_filter_than(filter1: Any | None, filter2: Any | None) -> bool:
    """
    Checks if ``filter1`` is arguments filter is a softer version of
    ``filter2`` argument filter. Softer means more results after
    filtering.

    Args:
        filter1: Argument filters to check. The convention is :code:`None == {}`
        filter2: Argument filters to check. The convention is :code:`None == {}`
    """
    if filter1 is None:
        return True
    if filter2 is None:
        if isinstance(filter1, dict) and len(filter1.keys()) == 0:
            return True
        return False
    if isinstance(filter1, dict):
        if not isinstance(filter2, dict):
            return False
        for key in filter1.keys():
            if not key in filter2:
                return False
            if not is_softer_filter_than(filter1[key], filter2[key]):
                return False
        return True
    if isinstance(filter1, list):
        if not isinstance(filter2, list):
            return False
        sb = set(filter1)
        sp = set(filter2)
        return sb.issubset(sp)
    return filter1 == filter2

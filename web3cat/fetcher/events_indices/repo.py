from typing import Any, Dict, Iterator, List

import json
from web3cat.fetcher.events_indices.index import EventsIndex
from web3cat.fetcher.core import Core


class EventsIndicesRepo(Core):
    """
    Reading and writing :class:`EventsIndex` to database.
    """

    def find_indices(
        self,
        address: str,
        event: str,
        args: Dict[str, Any] | None = None,
    ) -> Iterator[EventsIndex]:
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
        2. Stating that ``Transfer`` from address "0x6b17..." events were fetched
           from block 3000 to 4000

        Now we want to query if the ``Transfer`` events were fetched for
        :code:`{"from": "0x6b17..."}` for blocks 2500 to 4000. A naive implementation
        would return just the :code:`{"from": "0x6b17..."}` index stating that blocks
        2500 to 3000 are missing. However, these events were already fetched
        as part of all ``Transfer`` events from block 2000 to 4000.
        That's why a list of indices is returned, so the check is made
        against the data that is really missing.

        Args:
            address: Contract address
            event: Event name
            args: Argument filters

        Returns:
            Iterator of matched indices
        """
        rows = self.conn.execute(
            "SELECT * FROM events_indices WHERE chain_id = ? "
            "AND address = ? AND event = ?",
            (self.chain_id, address, event),
        )
        indices = (EventsIndex.from_row(r) for r in rows)
        return (i for i in indices if is_softer_filter_than(i.args, args))

    def get_index(
        self,
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
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM events_indices WHERE chain_id = ? AND address = ? "
            "AND event = ? and args = ?",
            (
                self.chain_id,
                address,
                event,
                json.dumps(EventsIndex.normalize_args(args)),
            ),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return EventsIndex.from_row(row)

    def save(self, indices: List[EventsIndex]):
        """
        Save indices to database.

        Args:
            indices: a list of indices to save
        """
        cursor = self.conn.cursor()
        rows = [i.to_row() for i in indices]
        statement = (
            "INSERT INTO events_indices VALUES (?,?,?,?,?) "
            "ON CONFLICT(chain_id,address,event,args) DO UPDATE SET data = excluded.data"
        )
        cursor.executemany(
            statement,
            rows,
        )

    def purge(self):
        """
        Clear all database entries
        """
        cursor = self.conn.cursor()
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
        return sp.issubset(sb)
    return filter1 == filter2

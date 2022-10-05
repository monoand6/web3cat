from typing import List
from fetcher.events.event import Event
from fetcher.db import Repo


class EventsRepo(Repo):
    """
    Reading and writing :class:`Event` to database.
    """

    def find(
        self,
        chain_id: int,
        event: str,
        address: str,
        from_block: int = 0,
        to_block: int = 2**32 - 1,
    ) -> List[Event]:
        """
        Find all events in the database.

        Args:
            chain_id: Ethereum chain_id
            event: Event name
            address: Contract address
            from_block: starting from this block (inclusive)
            to_block: ending with this block (non-inclusive)

        Returns:
            List of found events
        """
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM events WHERE chain_id = ? AND event = ? AND address = ? AND block_number >= ? AND block_number < ?",
            (chain_id, event, address.lower(), from_block, to_block),
        )
        rows = cursor.fetchall()
        return [Event.from_tuple(r) for r in rows]

    def save(self, events: List[Event]):
        """
        Save a set of events into the database.

        Args:
            events: List of events to save
        """
        cursor = self._connection.cursor()
        rows = [e.to_tuple() for e in events]
        cursor.executemany(
            "INSERT INTO events VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

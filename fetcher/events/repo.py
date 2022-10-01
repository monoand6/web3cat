from typing import List
from fetcher.db import DB
from fetcher.events.event import Event
from fetcher.repo import Repo


class EventsRepo(Repo):
    def find(
        self,
        chain_id: int,
        event: str,
        address: str,
        from_block: int = 0,
        to_block: int = 2**32 - 1,
    ) -> List[Event]:
        cursor = self._db.cursor()
        cursor.execute(
            "SELECT * FROM events WHERE chain_id = ? AND event = ? AND address = ? AND block_number >= ? AND block_number < ?",
            (chain_id, event, address.lower(), from_block, to_block),
        )
        rows = cursor.fetchall()
        return [Event.from_tuple(r) for r in rows]

    def save(self, events: List[Event]):
        cursor = self._db.cursor()
        rows = [e.to_tuple() for e in events]
        cursor.executemany(
            "INSERT INTO events VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

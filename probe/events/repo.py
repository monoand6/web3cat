from typing import List
from probe.db import DB
from probe.events.event import Event
from probe.repo import Repo


class EventsRepo(Repo):
    def find(
        self, event: str, address: str, from_block: int = 0, to_block: int = 2**32 - 1
    ) -> List[Event]:
        cursor = self._db.cursor()
        cursor.execute(
            "SELECT * FROM events WHERE event = ? AND address = ? AND block_number >= ? AND block_number < ?",
            (event, address, from_block, to_block),
        )
        rows = cursor.fetchall()
        return [Event.from_tuple(r) for r in rows]

    def save(self, events: List[Event]):
        cursor = self._db.cursor()
        rows = [e.to_tuple() for e in events]
        cursor.executemany(
            "INSERT INTO events VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )



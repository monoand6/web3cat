from typing import Any, Dict, Iterator, List, Tuple
from web3cat.fetcher.events.event import Event
from web3cat.fetcher.core import Core


class EventsRepo(Core):
    """
    Reading and writing :class:`Event` to database.
    """

    def find(
        self,
        event: str,
        address: str,
        argument_filters: Dict[str, Any] | None = None,
        from_block: int = 0,
        to_block: int = 2**32 - 1,
    ) -> Iterator[Event]:
        """
        Find all events in the database.

        Args:
            event: Event name
            address: Contract address
            argument_filters: an additional filter with keys being event fieds (AND query)
                              and values are filter values (list of values for OR query)
            from_block: starting from this block (inclusive)
            to_block: ending with this block (non-inclusive)

        Returns:
            Iterator over found events
        """
        cursor = self.conn.cursor()
        args_query, args_values = self._convert_filter_to_sql(argument_filters)
        statement = (
            "SELECT * FROM events WHERE chain_id = ? AND event = ? "
            f"AND address = ? AND block_number >= ? AND block_number < ?{args_query}"
        )
        args = tuple(
            [self.chain_id, event, address.lower(), from_block, to_block, *args_values]
        )
        cursor.execute(
            statement,
            args,
        )
        rows = cursor.fetchall()
        return (Event.from_row(r) for r in rows)

    def save(self, events: List[Event]):
        """
        Save a set of events into the database.

        Args:
            events: List of events to save
        """
        cursor = self.conn.cursor()
        rows = [e.to_row() for e in events]
        cursor.executemany(
            "INSERT INTO events VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clear all database entries
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM events")

    def _convert_filter_to_sql(
        self, event_filter: Dict[str, Any] | None
    ) -> Tuple[str, List[Any]]:
        if event_filter is None:
            return ("", [])
        query = ""
        values = []
        for k, v in event_filter.items():
            if not isinstance(v, list):
                v = [v]
            inner = " OR ".join([f"""json_extract(args, "$.{k}") = ?""" for _ in v])
            query += f" AND ({inner})"
            values += v
        return (query, values)

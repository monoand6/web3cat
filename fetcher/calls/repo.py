from typing import Any, Dict, Iterator, List
from fetcher.calls.call import Call
from fetcher.core import Core


class CallsRepo(Core):
    """
    Reading and writing :class:`Call` to database.
    """

    def find(
        self,
        address: str,
        calldata: str,
        from_block: int = 0,
        to_block: int = 2**32 - 1,
    ) -> Iterator[Call]:
        """
        Find all calls in the database.

        Args:
            chain_id: Ethereum chain_id
            address: Contract address
            calldata: Ethereum calldata
            from_block: starting from this block (inclusive)
            to_block: ending with this block (non-inclusive)

        Returns:
            List of found calls
        """
        rows = self.conn.execute(
            "SELECT * FROM calls WHERE chain_id = ? AND address = ? AND calldata = ? AND block_number >= ? AND block_number < ?",
            (self.chain_id, address.lower(), calldata, from_block, to_block),
        )
        return (Call.from_tuple(r) for r in rows)

    def save(self, calls: List[Call]):
        """
        Save a set of calls into the database.

        Args:
            calls: List of calls to save
        """
        cursor = self.conn.cursor()
        rows = [e.to_tuple() for e in calls]
        cursor.executemany(
            "INSERT INTO calls VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clean all database entries
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM calls")

from typing import Any, Dict, Iterator, List
from fetcher.balances.balance import Balance
from fetcher.db import Repo


class BalancesRepo(Repo):
    """
    Reading and writing :class:`Balance` to database.
    """

    def find(
        self,
        chain_id: int,
        address: str,
        from_block: int = 0,
        to_block: int = 2**32 - 1,
    ) -> Iterator[Balance]:
        """
        Find all balances in the database.

        Args:
            chain_id: Ethereum chain_id
            address: Contract address
            from_block: starting from this block (inclusive)
            to_block: ending with this block (non-inclusive)

        Returns:
            List of found balances
        """
        rows = self._connection.execute(
            "SELECT * FROM balances WHERE chain_id = ? AND address = ? AND block_number >= ? AND block_number < ?",
            (chain_id, address.lower(), from_block, to_block),
        )
        return (Balance.from_tuple(r) for r in rows)

    def save(self, balances: List[Balance]):
        """
        Save a set of balances into the database.

        Args:
            balances: List of balances to save
        """
        cursor = self._connection.cursor()
        rows = [e.to_tuple() for e in balances]
        cursor.executemany(
            "INSERT INTO balances VALUES(?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clean all database entries
        """
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM balances")

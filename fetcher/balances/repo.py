from typing import Any, Dict, Iterator, List
from fetcher.balances.balance import Balance
from fetcher.core import Repo


class BalancesRepo(Repo):
    """
    Reading and writing :class:`Balance` to database cache.
    """

    def find(
        self,
        chain_id: int,
        address: str,
        from_block: int = 0,
        to_block: int = 2**32 - 1,
    ) -> Iterator[Balance]:
        """
        Find all balances in the database cache.

        Args:
            chain_id: Ethereum chain_id
            address: Contract / EOA address
            from_block: starting from this block (inclusive)
            to_block: ending with this block (non-inclusive)

        Returns:
            An iterator over found balances
        """
        rows = self.conn.execute(
            "SELECT * FROM balances WHERE chain_id = ? AND address = ? AND block_number >= ? AND block_number < ?",
            (chain_id, address.lower(), from_block, to_block),
        )
        return (Balance.from_tuple(r) for r in rows)

    def save(self, balances: List[Balance]):
        """
        Save a list of balances into the database cache.

        Args:
            balances: List of balances to save
        """
        rows = [e.to_tuple() for e in balances]
        self.conn.executemany(
            "INSERT INTO balances VALUES(?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clean all balances entries from the database cache.
        """
        self.conn.execute("DELETE FROM balances")

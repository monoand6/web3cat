from typing import Iterator, List, Tuple
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
            "SELECT * FROM calls WHERE chain_id = ? AND address = ? "
            "AND calldata = ? AND block_number >= ? AND block_number < ?",
            [self.chain_id, address.lower(), calldata, from_block, to_block],
        )
        return (Call.from_row(r) for r in rows)

    def find_many(
        self,
        addresses_and_calldatas: List[Tuple[str, str]],
        blocks: List[int],
    ) -> Iterator[Call]:
        """
        Find all calls in the database.

        Args:
            addresses_and_calldatas: A list of addresses and calldatas
            blocks: A list of blocks

        Returns:
            A list of calls
        """
        if len(addresses_and_calldatas) == 0 or len(blocks) == 0:
            return []
        address_statement = " OR ".join(
            ["(address = ? AND calldata = ?)"] * len(addresses_and_calldatas)
        )
        address_args = [item.lower() for ac in addresses_and_calldatas for item in ac]
        blocks_statement = ",".join(["?"] * len(blocks))
        statement = (
            f"SELECT * FROM calls WHERE chain_id = ? AND ({address_statement}) "
            f"AND block_number IN ({blocks_statement})"
        )
        rows = self.conn.execute(
            statement,
            [self.chain_id, *address_args, *blocks],
        )
        return (Call.from_row(r) for r in rows)

    def save(self, calls: List[Call]):
        """
        Save a list of calls into the database.

        Args:
            calls: List of calls to save
        """
        rows = [e.to_row() for e in calls]
        self.conn.executemany(
            "INSERT INTO calls VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clear all database entries
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM calls")

from typing import Iterator, List, Tuple
from fetcher.blocks.block import Block
from fetcher.db import Repo


class BlocksRepo(Repo):
    """
    Reading and writing :class:`Block` to database.
    """

    def get_block_after_timestamp(self, chain_id: int, timestamp: int) -> Block | None:
        """
        Get the first block after the timestamp in database.

        Args:
            chain_id: Ethereum chain_id
            timestamp: UNIX timestamp, UTC+0

        Returns:
            First block after the timestamp, ``None`` if the block doesn't exist
        """

        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM blocks WHERE timestamp >= ? AND chain_id = ? ORDER BY block_number LIMIT 1",
            (timestamp, chain_id),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return Block.from_tuple(row)

    def get_block_before_timestamp(self, chain_id: int, timestamp: int) -> Block | None:
        """
        Get the first block before the timestamp in database.

        Args:
            chain_id: Ethereum chain_id
            timestamp: UNIX timestamp, UTC+0

        Returns:
            First block before the timestamp, ``None`` if the block doesn't exist
        """

        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM blocks WHERE timestamp < ? AND chain_id = ? ORDER BY block_number DESC LIMIT 1",
            (timestamp, chain_id),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return Block.from_tuple(row)

    def find(self, chain_id: int, blocks: int | List[int]) -> List[Block]:
        """
        Find blocks by number

        Args:
            chain_id: Ethereum chain_id
            blocks: block number or a list of block numbers

        Returns:
            A list of found blocks
        """
        int_blocks = blocks if isinstance(blocks, list) else [blocks]
        cursor = self._connection.cursor()
        out = []
        if len(int_blocks) > 0:
            statement = f"SELECT * FROM blocks WHERE block_number IN ({','.join('?' * len(int_blocks))}) AND chain_id = ?"
            cursor.execute(
                statement,
                int_blocks + [chain_id],
            )
            out = cursor.fetchall()
        out = [Block.from_tuple(b) for b in out]
        # unique
        out = list({(b.number): b for b in out}.values())
        return sorted(out, key=lambda x: x.number)

    def all(self, chain_id: int) -> List[Block]:
        blocks = self._connection.execute(
            "SELECT * FROM blocks WHERE chain_id = ?", (chain_id,)
        )
        out = [Block.from_tuple(b) for b in blocks]
        out = list({(b.number): b for b in out}.values())
        return sorted(out, key=lambda x: x.number)

    def save(self, blocks: List[Block]):
        """
        Save a set of blocks into the database.

        Args:
            blocks: List of blocks to save
        """

        cursor = self._connection.cursor()
        rows = [b.to_tuple() for b in blocks]
        cursor.executemany(
            "INSERT INTO blocks VALUES(?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clean all database entries
        """
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM blocks")

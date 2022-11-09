from typing import Iterator, List
from web3cat.fetcher.blocks.block import Block
from web3cat.fetcher.core import Core


class BlocksRepo(Core):
    """
    Reading and writing :class:`Block` to database.
    """

    def get_block_after_timestamp(self, timestamp: int) -> Block | None:
        """
        Get the first block strictly after the timestamp in database.

        Args:
            chain_id: Ethereum chain_id
            timestamp: UNIX timestamp

        Returns:
            First block after the timestamp, ``None`` if the block doesn't exist
        """

        row = self.conn.execute(
            "SELECT * FROM blocks WHERE timestamp > ? AND chain_id = ? "
            "ORDER BY block_number LIMIT 1",
            (timestamp, self.chain_id),
        ).fetchone()
        if not row:
            return None
        return Block.from_row(row)

    def get_block_before_timestamp(self, timestamp: int) -> Block | None:
        """
        Get the first block at or before the timestamp in database.

        Args:
            chain_id: Ethereum chain_id
            timestamp: UNIX timestamp, UTC+0

        Returns:
            First block before the timestamp, ``None`` if the block doesn't exist
        """

        row = self.conn.execute(
            "SELECT * FROM blocks WHERE timestamp < ? AND chain_id = ? "
            "ORDER BY block_number DESC LIMIT 1",
            (timestamp, self.chain_id),
        ).fetchone()
        if not row:
            return None
        return Block.from_row(row)

    def find(self, blocks: int | List[int]) -> Iterator[Block]:
        """
        Find blocks by number

        Warning:
            The order of the blocks are not preserved. Duplicates are eliminated.

        Args:
            blocks: block number or a list of block numbers

        Returns:
            A list of found blocks
        """
        int_blocks = blocks if isinstance(blocks, list) else [blocks]
        int_blocks = list(set(int_blocks))
        if len(int_blocks) == 0:
            return iter([])

        statement = (
            "SELECT * FROM blocks WHERE block_number IN "
            f" ({','.join('?' * len(int_blocks))}) AND chain_id = ?"
        )
        out = self.conn.execute(
            statement,
            int_blocks + [self.chain_id],
        )
        out = (Block.from_row(b) for b in out)
        return sorted(out, key=lambda x: x.number)

    def all(self) -> Iterator[Block]:
        """
        All blocks in the database
        """
        blocks = self.conn.execute(
            "SELECT * FROM blocks WHERE chain_id = ?", (self.chain_id,)
        )
        return (Block.from_row(b) for b in blocks)

    def save(self, blocks: List[Block]):
        """
        Save a set of blocks into the database.

        Args:
            blocks: List of blocks to save
        """

        cursor = self.conn.cursor()
        rows = [b.to_row() for b in blocks]
        cursor.executemany(
            "INSERT INTO blocks VALUES(?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clear all database entries
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM blocks")

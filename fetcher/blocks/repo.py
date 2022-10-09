from typing import List, Tuple
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

    def find(self, chain_id: int, blocks: int | str | List[int | str]) -> List[Block]:
        """
        Find blocks by number or hash

        Args:
            chain_id: Ethereum chain_id
            blocks: block number or block hash or a list of block numbers or hashes

        Returns:
            A list of found blocks
        """
        int_blocks, str_blocks = self._resolve_blocks_args(blocks)
        cursor = self._connection.cursor()
        int_blocks_res = []
        str_blocks_res = []
        if len(int_blocks) > 0:
            statement = f"SELECT * FROM blocks WHERE block_number IN ({','.join('?' * len(int_blocks))}) AND chain_id = ?"
            cursor.execute(
                statement,
                int_blocks + [chain_id],
            )
            int_blocks_res = cursor.fetchall()
        if len(str_blocks) > 0:
            statement = f"SELECT * FROM blocks WHERE block_hash IN ({','.join('?' * len(str_blocks))}) AND chain_id = ?"
            cursor.execute(
                statement,
                str_blocks + [chain_id],
            )
            str_blocks_res = cursor.fetchall()
        blocks_res = int_blocks_res + str_blocks_res
        blocks_res = [Block.from_tuple(b) for b in blocks_res]
        # unique
        blocks_res = list({(b.number): b for b in blocks_res}.values())
        return sorted(blocks_res, key=lambda x: x.number)

    def save(self, blocks: List[Block]):
        """
        Save a set of blocks into the database.

        Args:
            blocks: List of blocks to save
        """

        cursor = self._connection.cursor()
        rows = [b.to_tuple() for b in blocks]
        cursor.executemany(
            "INSERT INTO blocks VALUES(?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clean all database entries
        """
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM blocks")

    def _resolve_blocks_args(
        self, blocks: int | str | List[int | str]
    ) -> Tuple[List[int], List[str]]:
        if type(blocks) == int:
            return [[blocks], []]
        if type(blocks) == str:
            return [[], [blocks]]
        int_res, str_res = [], []
        for b in blocks:
            if type(b) == int:
                int_res.append(b)
                continue
            if type(b) == str:
                str_res.append(b)
                continue
            raise Exception(f"Unsupported block type {type(b)} for block {b}")
        return [int_res, str_res]

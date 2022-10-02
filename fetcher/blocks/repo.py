from typing import List, Tuple
from fetcher.db import DB
from fetcher.blocks.block import Block
from fetcher.db import Repo


class BlocksRepo(Repo):
    def get_block_after_timestamp(self, timestamp: int, chain_id: int) -> Block | None:
        cursor = self._db.cursor()
        cursor.execute(
            "SELECT * FROM blocks WHERE timestamp >= ? AND chain_id = ? ORDER BY block_number LIMIT 1",
            (timestamp, chain_id),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return Block.from_tuple(row)

    def get_block_before_timestamp(self, timestamp: int, chain_id: int) -> Block | None:
        cursor = self._db.cursor()
        cursor.execute(
            "SELECT * FROM blocks WHERE timestamp < ? AND chain_id = ? ORDER BY block_number DESC LIMIT 1",
            (timestamp, chain_id),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return Block.from_tuple(row)

    def find(
        self, blocks: int | str | List[int | str], chain_id: int
    ) -> Tuple[List[Block]]:
        int_blocks, str_blocks = self._resolve_blocks_args(blocks)
        cursor = self._db.cursor()
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

    def read(self, blocks: List[Block]):
        cursor = self._db.cursor()
        rows = [b.to_tuple() for b in blocks]
        cursor.executemany(
            "INSERT INTO blocks VALUES(?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()

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

from __future__ import annotations

import json
from fetcher.blocks.repo import BlocksRepo
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.auto import w3 as w3auto

from fetcher.blocks.block import Block
from fetcher.w3_utils import json_response
from fetcher.db import DB


class BlocksService:
    _blocksRepo: BlocksRepo
    _w3: Web3
    _chain_id: int
    _block_time_est: float

    def __init__(self, blocks_db: BlocksRepo, w3: Web3):
        self._blocks_db = blocks_db
        self._w3 = w3
        self._chain_id = w3.eth.chain_id
        self._block_time_est = 1.0
        if self._chain_id in [1, 3, 4, 5, 42]:
            self._block_time_est = 13.0

    def create(
        cache_path: str = "cache.sqlite3", rpc: str | None = None
    ) -> BlocksService:
        db = DB.from_path(cache_path)
        blocks_repo = BlocksRepo(db)
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
        return BlocksService(blocks_repo, w3)

    def get_latest_block(self) -> Block:
        return self.get_block()

    def get_block_right_after_timestamp(self, timestamp: int) -> Block | None:
        ts = timestamp

        # right_block is guaranteed to be after the timestamp
        right_block = self._blocks_db.get_block_after_timestamp(ts, self._chain_id)
        if right_block is None:
            right_block = self.get_block()

        if right_block.timestamp < timestamp:  # no blocks exist after the timestamp
            return None

        left_block = self._blocks_db.get_block_before_timestamp(ts, self._chain_id)
        if left_block is None:
            # harsh approximation but once it's run a single time
            # a better approximation from db will come on each
            # subsequent call
            left_block = self.get_block(1)

        # Time stamp is before the chain genesis
        if left_block.timestamp >= timestamp:
            return left_block

        # initial esitmates for blocks are set
        # invariant: left_block.timestamp < timestamp <= right_block.timestamp
        while right_block.number - left_block.number > 1:
            num = (right_block.number + left_block.number) // 2
            block = self.get_block(num)
            if block.timestamp >= timestamp:
                right_block = block
            else:
                left_block = block
        return right_block

    def get_block(self, number: int | None = None) -> Block | None:
        """
        number = None => fetch latest
        """
        if number:
            blocks = self._blocks_db.find(number, self._chain_id)
            if len(blocks) > 0:
                return blocks[0]
        raw_block = None
        try:
            raw_block = self._w3.eth.get_block(number or "latest")
            raw_block = json.loads(json_response(raw_block))
        except BlockNotFound:
            return None

        block = Block(
            chain_id=self._chain_id,
            hash=raw_block["hash"],
            number=raw_block["number"],
            timestamp=raw_block["timestamp"],
        )
        self._blocks_db.read([block])
        self._blocks_db.commit()
        return block

from db import BlocksDB
from web3 import Web3
from web3.exceptions import BlockNotFound

from probe.model import Block


class Blocks:
    _blocksDB: BlocksDB
    _w3: Web3
    _chain_id: int
    _block_time_est: float

    def __init__(self, blocks_db: BlocksDB, w3: Web3):
        self._blocks_db = blocks_db
        self._w3 = w3
        self._chain_id = w3.eth.chain_id
        self._block_time_est = 1.0
        if self._chain_id in [1, 3, 4, 5, 42]:
            self._block_time_est = 13.0

    def get_latest_block(self) -> Block:
        return self._get_block()

    def get_block_right_after_timestamp(self, timestamp: int) -> Block | None:
        ts = timestamp

        # right_block is guaranteed to be after the timestamp
        right_block = self._blocks_db.get_block_after_timestamp(ts)
        if not right_block:
            right_block = self._get_block()

        if right_block.timestamp < timestamp:  # no blocks exist after the timestamp
            return None

        left_block = self._blocks_db.get_block_before_timestamp(ts)
        if not left_block:
            # harsh approximation but once it's run a single time
            # a better approximation from db will come on each
            # subsequent call
            left_block = self._get_block(1)

        # initial esitmates for blocks are set
        # invariant: left_block.timestamp < timestamp <= right_block.timestamp
        while right_block.number - left_block.number > 1:
            num = (right_block.number + left_block.number) // 2
            block = self._get_block(num)
            if block.timestamp >= timestamp:
                right_block = block
            else:
                left_block = block
        return right_block

    def _get_block(self, number: int | None) -> Block | None:
        """
        number = None => fetch latest
        """
        block = None
        if number:
            block = self._blocks_db.read_blocks(number, self._chain_id)
        if block is not None:
            return block
        raw_block = None
        try:
            raw_block = self._w3.eth.get_block(number | "latest")
        except BlockNotFound:
            return None

        block = Block(
            chain_id=self._chain_id,
            hash=raw_block["hash"],
            number=raw_block["number"],
            timestamp=raw_block["timestamp"],
        )
        self._blocks_db.write_blocks([block])
        self._blocks_db.commit()
        return block

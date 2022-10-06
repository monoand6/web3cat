from __future__ import annotations

import json
from fetcher.blocks.repo import BlocksRepo
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.auto import w3 as w3auto

from fetcher.blocks.block import Block
from fetcher.db import connection_from_path
from fetcher.utils import json_response


class BlocksService:
    """
    Service for fetching Ethereum block data.

    The sole purpose of this service is to fetch events from web3, cache them,
    and save from the cache on subsequent calls.


    The exact flow goes like this:
    ::

                +---------------+            +-------+ +-------------+
                | BlocksService |            | Web3  | | BlocksRepo  |
                +---------------+            +-------+ +-------------+
        -----------------  |                        |            |
        | Request blocks |-|                        |            |
        |----------------| |                        |            |
                           |                        |            |
                           | Get blocks             |            |
                           |------------------------------------>|
                           |                        |            |
                           | Get missing blocks     |            |
                           |----------------------->|            |
                           |                        |            |
                           | Save missing blocks    |            |
                           |------------------------------------>|
              -----------  |                        |            |
              | Response |-|                        |            |
              |----------| |                        |            |
                           |                        |            |

    """

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

    @staticmethod
    def create(
        cache_path: str = "cache.sqlite3", rpc: str | None = None
    ) -> BlocksService:
        """
        Create an instance of :class:`BlocksService`

        Args:
            cache_path: path for the cache database
            rpc: Ethereum rpc url. If :code:`None`, `Web3 auto detection <https://web3py.savethedocs.io/en/stable/providers.html#how-automated-detection-works>`_ is used

        Returns:
            An instance of :class:`BlocksService`
        """

        conn = connection_from_path(cache_path)
        blocks_repo = BlocksRepo(conn)
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
        return BlocksService(blocks_repo, w3)

    def get_latest_block(self) -> Block:
        """
        Get the latest block from Ethereum
        """
        return self.get_block()

    def get_block_right_after_timestamp(self, timestamp: int) -> Block | None:
        """
        Get the first block after a timestamp.

        Args:
            timestamp: UNIX timestamp, UTC+0

        Returns:
            First block after timestamp, :code:`None` if the block doesn't exist
        """
        ts = timestamp

        # right_block is guaranteed to be after the timestamp
        right_block = self._blocks_db.get_block_after_timestamp(self._chain_id, ts)
        if right_block is None:
            right_block = self.get_block()

        if right_block.timestamp < timestamp:  # no blocks exist after the timestamp
            return None

        left_block = self._blocks_db.get_block_before_timestamp(self._chain_id, ts)
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
        Get block with a specific number.

        Args:
            number: block number. If :code:`None`, fetches the latest block

        Returns:
            Block with this number. :code:`None` if the block doesn't exist.
        """
        if number:
            blocks = self._blocks_db.find(self._chain_id, number)
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
        self._blocks_db.save([block])
        self._blocks_db.commit()
        return block

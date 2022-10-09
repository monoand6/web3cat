from __future__ import annotations

import json
import numbers
from typing import List
from fetcher.blocks.repo import BlocksRepo
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.auto import w3 as w3auto

from fetcher.blocks.block import Block
from fetcher.db import connection_from_path
from fetcher.utils import json_response, print_progress


DEFAULT_BLOCK_TIMESTAMP_GRID = 1000


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
            rpc: Ethereum rpc url. If ``None``, `Web3 auto detection <https://web3py.savethedocs.io/en/stable/providers.html#how-automated-detection-works>`_ is used

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
            First block after timestamp, ``None`` if the block doesn't exist
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

    def get_block_timestamps(
        self, block_numbers: List[int], grid_step: int = DEFAULT_BLOCK_TIMESTAMP_GRID
    ) -> List[int]:
        """
        Get timestamps for block numbers.

        Note that by default these timestamps are not 100% accurate.
        The use case for this function is to provide timestamps for
        events. However, fetching a block for every events might demand
        a fair amount of rpc calls.

        That's why the following algorithm is proposed. We make a block
        number grid with width specified by the ``grid_step`` parameter.
        """
        blocks_index = {}
        for bn in block_numbers:
            if grid_step == 0 or bn % grid_step == 0:
                blocks_index[bn] = None
                continue
            rounded = bn - bn % grid_step
            blocks_index[rounded] = None
            blocks_index[rounded + grid_step] = None

        cached_blocks: List[Block] = self._blocks_db.find(
            self._chain_id, list(blocks_index.keys())
        )
        for b in cached_blocks:
            blocks_index[b.number] = b.timestamp
        block_numbers_for_fetch = []
        for bn in blocks_index.keys():
            if blocks_index[bn] is None:
                block_numbers_for_fetch.append(bn)
        fetched_blocks = self._fetch_many_blocks_and_save(block_numbers_for_fetch)
        for b in fetched_blocks:
            blocks_index[b.number] = b.timestamp
        res = []
        for bn in block_numbers:
            if grid_step == 0 or bn % grid_step == 0:
                res.append(blocks_index[bn])
                continue
            rounded = bn - bn % grid_step
            w = (bn % grid_step) / grid_step
            ts = int(
                blocks_index[rounded] * (1 - w) + blocks_index[rounded + grid_step] * w
            )
            res.append(ts)
        return res

    def get_block(self, number: int | None = None) -> Block | None:
        """
        Get block with a specific number.

        Args:
            number: block number. If ``None``, fetches the latest block

        Returns:
            Block with this number. ``None`` if the block doesn't exist.
        """
        if number:
            blocks = self._blocks_db.find(self._chain_id, number)
            if len(blocks) > 0:
                return blocks[0]
        block = self._fetch_block_and_save(number)
        return block

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._blocksRepo.purge()
        self._blocksRepo.commit()

    def _fetch_many_blocks_and_save(self, numbers: List[int]) -> List[Block]:
        if len(numbers) == 0:
            return []
        prefix = f"Fetching {len(numbers)} blocks"
        blocks = []
        for i, n in enumerate(numbers):
            print_progress(i, len(numbers), prefix=prefix)
            block = self._fetch_block(n)
            blocks.append(block)
            self._blocks_db.save([block])
            self._blocks_db.commit()
        print_progress(len(numbers), len(numbers), prefix=prefix)
        return blocks

    def _fetch_block_and_save(self, number: int | None) -> Block | None:
        block = self._fetch_block(number)
        self._blocks_db.save([block])
        self._blocks_db.commit()
        return block

    def _fetch_block(self, number: int | None) -> Block | None:
        block_id = "lasest" if number is None else number
        try:
            raw_block = self._w3.eth.get_block(block_id)
            block = json.loads(json_response(raw_block))
            return Block(
                chain_id=self._chain_id,
                hash=block["hash"],
                number=block["number"],
                timestamp=block["timestamp"],
            )

        except BlockNotFound:
            return None

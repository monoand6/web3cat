from __future__ import annotations
from datetime import datetime

import json
from typing import List
from fetcher.blocks.repo import BlocksRepo
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.auto import w3 as w3auto
from math import log

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

    Grid steps

    Note that by default, these timestamps are not 100% accurate.
    Why's that?

    Consider the primary use case for this function: providing timestamps
    for events. Fetching a block for every event might demand
    a fair amount of rpc calls.

    That's why the following algorithm is proposed:

        1. We make a block number grid with a width specified by the ``grid_step`` parameter.
        2. For each block number, we take the two closest grid blocks (below and above).
        3. Fetch the grid blocks
        4. Assume :math:`a_n` and :math:`a_t` is a number and a timestamp for the block above
        5. Assume :math:`b_n` and :math:`b_t` is a number and a timestamp for the block below
        6. Assume :math:`c_n` and :math:`c_t` is a number and a timestamp for the block we're looking for
        7. :math:`w = (c_n - b_n) / (a_n - b_n)`
        8. Then :math:`c_t = b_t \cdot (1-w) + a_t * w`

    This algorithm gives a reasonably good approximation for the block
    timestamp and considerably reduces the number of fetches.
    For example, if we have 500 events happening in the 1000 - 2000
    block range, then we fetch only two blocks instead of 500.

    If you still want the exact precision, use
    ``grid_step = 0``.

    Warning:
        It's highly advisable to use a single ``grid_step`` for all data.
        Otherwise (in theory) the happens-before relationship might
        be violated for the data points.


    """

    _blocks_repo: BlocksRepo
    _w3: Web3
    _chain_id: int
    _grid_step: int

    def __init__(
        self,
        blocks_repo: BlocksRepo,
        w3: Web3,
        grid_step: int,
    ):
        self._blocks_repo = blocks_repo
        self._w3 = w3
        self._chain_id = w3.eth.chain_id
        self._grid_step = grid_step

    @staticmethod
    def create(
        grid_step: int = DEFAULT_BLOCK_TIMESTAMP_GRID,
        cache_path: str = "cache.sqlite3",
        rpc: str | None = None,
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
        return BlocksService(blocks_repo, w3, grid_step)

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
        right_block = self._blocks_repo.get_block_after_timestamp(self._chain_id, ts)

        # snapping right block to grid (either multiple of grid_step or latest block)
        if right_block is None:
            right_block = self.get_block()

        if right_block.timestamp < timestamp:  # no blocks exist after the timestamp
            return None

        right_block = self._get_closest_right_grid_block(right_block, self._grid_step)

        left_block = self._blocks_repo.get_block_before_timestamp(self._chain_id, ts)
        if left_block is None:
            # harsh approximation but once it's run a single time
            # a better approximation from db will come on each
            # subsequent call
            left_block = self.get_block(1)

        # Time stamp is before the chain genesis
        if left_block.timestamp > timestamp:
            return None

        left_block = self._get_closest_left_grid_block(left_block, self._grid_step)

        if right_block.number - left_block.number <= self._grid_step:
            return self._get_interpolated_block_number_right_after_timestamp(
                timestamp, left_block, right_block
            )

        estimated_hops = int(
            log((right_block.number - left_block.number) / self._grid_step, 2)
        )
        hops = 0
        prefix = f"Finding block for {datetime.fromtimestamp(timestamp).isoformat()}"

        # initial esitmates for blocks are set
        # invariant: left_block.timestamp < timestamp <= right_block.timestamp
        while right_block.number - left_block.number > self._grid_step:
            # print_progress(min(hops - 1, estimated_hops), estimated_hops, prefix)

            hops += 1
            w = (timestamp - left_block.timestamp) / (
                right_block.timestamp - left_block.timestamp
            )
            # snapped to grid
            num = (
                int(left_block.number * (1 - w) + right_block.number * w)
                // self._grid_step
                * self._grid_step
            )
            # we don't query 0 block, the minimal block is 1
            if num == 0:
                num = 1
            if num == left_block.number:
                num += self._grid_step
            elif num == right_block.number:
                num -= self._grid_step
            block = self.get_block(num)
            if block.timestamp >= timestamp:
                right_block = block
            else:
                left_block = block

        # print_progress(estimated_hops, estimated_hops, prefix)
        bn = self._get_interpolated_block_number_right_after_timestamp(
            timestamp, left_block, right_block
        )
        return self.get_block(bn)

    def get_block_numbers(self, block_timestamps: List[int]) -> List[int]:
        out = []
        for ts in block_timestamps:
            block = self.get_block_right_after_timestamp(ts)
            b_left = block.number // self._grid_step * self._grid_step
            b_right = b_left + self._grid_step
            w = (block.number - b_left) / (b_right - b_left)
            out.append(int(b_left + w * (b_right - b_left)))
        return out

    def get_block_timestamps(self, block_numbers: List[int]) -> List[int]:
        """
        Get timestamps for block numbers.

        Args:
            block_numbers: the block numbers for resolving timestamps

        Returns:
            A list of block timestamps
        """
        blocks_index = {}
        for bn in block_numbers:
            if self._grid_step == 0 or bn % self._grid_step == 0:
                blocks_index[bn] = None
                continue
            rounded = bn - bn % self._grid_step
            blocks_index[rounded] = None
            blocks_index[rounded + self._grid_step] = None

        cached_blocks: List[Block] = self._blocks_repo.find(
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
        out = []
        for bn in block_numbers:
            if self._grid_step == 0 or bn % self._grid_step == 0:
                out.append(blocks_index[bn])
                continue
            rounded = bn - bn % self._grid_step
            w = (bn % self._grid_step) / self._grid_step
            ts = int(
                blocks_index[rounded] * (1 - w)
                + blocks_index[rounded + self._grid_step] * w
            )
            out.append(ts)
        return out

    def get_block(self, number: int | None = None) -> Block | None:
        """
        Get block with a specific number.

        Args:
            number: block number. If ``None``, fetches the latest block

        Returns:
            Block with this number. ``None`` if the block doesn't exist.
        """
        if not number is None:
            blocks = self._blocks_repo.find(self._chain_id, number)
            if len(blocks) > 0:
                return blocks[0]
        block = self._fetch_block_and_save(number)
        return block

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._blocks_repo.purge()
        self._blocks_repo.commit()

    def _get_interpolated_block_number_right_after_timestamp(
        self, timestamp: int, left_block: Block, right_block: Block
    ) -> int:
        """
        Invariant: left_block.timestamp <= timestamp < right_block.timestamp
        """
        w = (timestamp - left_block.timestamp) / (
            right_block.timestamp - left_block.timestamp
        )
        return int(left_block.number * (1 - w) + right_block.number * w) + 1

    def _get_closest_right_grid_block(self, block: Block, grid_step: int) -> Block:
        mod = block.number % grid_step
        # if we're on the grid, we're good, otherwise get nearest grid block:
        if mod == 0:
            return block

        right_block_number = block.number - mod + grid_step
        right_block = self.get_block(right_block_number)
        if right_block is None:
            right_block = self.get_block()
        return right_block

    def _get_closest_left_grid_block(self, block: Block, grid_step: int) -> Block:
        mod = block.number % grid_step
        # if we're on the grid, we're good, otherwise get nearest grid block:
        if mod == 0:
            return block

        left_block_number = max(block.number - mod, 1)
        return self.get_block(left_block_number)

    def _fetch_many_blocks_and_save(self, numbers: List[int]) -> List[Block]:
        if len(numbers) == 0:
            return []
        prefix = f"Fetching {len(numbers)} blocks"
        blocks = []
        for i, n in enumerate(numbers):
            print_progress(i, len(numbers), prefix=prefix)
            block = self._fetch_block(n)
            blocks.append(block)
            self._blocks_repo.save([block])
            self._blocks_repo.commit()
        print_progress(len(numbers), len(numbers), prefix=prefix)
        return blocks

    def _fetch_block_and_save(self, number: int | None) -> Block | None:
        block = self._fetch_block(number)
        if block is None:
            return None
        self._blocks_repo.save([block])
        self._blocks_repo.commit()
        return block

    def _fetch_block(self, number: int | None) -> Block | None:
        block_id = "latest" if number is None else number
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

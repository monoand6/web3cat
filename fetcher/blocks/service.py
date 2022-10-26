from __future__ import annotations

import json
from typing import Dict, List, Literal
from web3.exceptions import BlockNotFound

from fetcher.blocks.repo import BlocksRepo
from fetcher.blocks.block import Block
from fetcher.core import Core
from fetcher.utils import json_response, print_progress


class BlocksService(Core):
    """
    Service for fetching and caching Ethereum block data.

    **Request/Response flow**

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

    Args:
        blocks_repo: An instance of :class:`fetcher.blocks.BlocksRepo`
        kwargs: Args for the :class:`fetcher.core.Core`

    See Also:
        :class:`fetcher.core.Core` for defining the block grid and
        how timestamps are approximated.

    """

    _blocks_repo: BlocksRepo
    _latest_block: Block | None
    _block_cache: Dict[int, Block]
    _block_grid_cache: Dict[int, Block]

    def __init__(self, blocks_repo: BlocksRepo, **kwargs):
        super().__init__(**kwargs)
        self._blocks_repo = blocks_repo
        self._latest_block = None
        self._block_cache = {}
        self._block_grid_cache = {}

    @staticmethod
    def create(**kwargs) -> BlocksService:
        """
        Create an instance of :class:`BlocksService`

        Args:
            kwargs: Args for the :class:`fetcher.core.Core`

        Returns:
            An instance of :class:`BlocksService`
        """
        blocks_repo = BlocksRepo(**kwargs)
        service = BlocksService(blocks_repo)
        return service

    @property
    def latest_block(self) -> Block:
        """
        Latest block from Ethereum (this value is cached on the first call)
        """
        if self._latest_block is None:
            self._latest_block = self._fetch_block_from_rpc()
        return self._latest_block

    def get_latest_block_at_timestamp(self, timestamp: int) -> Block | None:
        """
        Get the first block after a timestamp.

        Args:
            timestamp: UNIX timestamp

        Returns:
            First block after timestamp, ``None`` if the block doesn't exist
        """
        left_block = self._blocks_repo.get_block_before_timestamp(timestamp)
        if left_block is None:
            left_block = self._get_grid_block(1)

        right_block = self._blocks_repo.get_block_after_timestamp(timestamp)
        if right_block is None:
            right_block = self.latest_block

        if timestamp >= right_block.timestamp:
            return self.latest_block
        if timestamp < left_block.timestamp:
            return None

        # invariant: left_block.timestamp <= timestamp < right_block.timestamp
        while right_block.number - left_block.number > self.block_grid_step:
            w = (timestamp - left_block.timestamp) / (
                right_block.timestamp - left_block.timestamp
            )
            num = self._snap_to_grid(
                int((1 - w) * left_block.number + w * right_block.number),
                direction="left",
            )
            # out block somewhere close to left_block
            if num == left_block.number:
                num = self._next_grid_block(num)

            block = self._get_grid_block(num)
            if block.timestamp > timestamp:
                right_block = block
            else:
                left_block = block

        return self._synthesize_block_from_timestamp(left_block, right_block, timestamp)

    def get_latest_blocks_by_timestamps(
        self, block_timestamps: int | List[int]
    ) -> List[Block]:
        """
        Get a list of latest blocks as of timestamp.

        Args:
            block_timestamps: Unix timestamps

        Returns:
            A list of blocks in the same order
        """
        if not isinstance(block_timestamps, list):
            block_timestamps = [block_timestamps]

        if len(block_timestamps) == 0:
            return []
        block_idx = {}
        timestamps_to_fetch = []
        for timestamp in block_timestamps:
            block = self._resolve_from_cache(timestamp)
            if block is None:
                timestamps_to_fetch.append(timestamp)
            else:
                block_idx[timestamp] = block
        for i, timestamp in enumerate(timestamps_to_fetch):
            print_progress(
                i,
                len(timestamps_to_fetch),
                f"Resolving {len(timestamps_to_fetch)} block numbers",
            )
            block_idx[timestamp] = self.get_latest_block_at_timestamp(timestamp)

        if len(timestamps_to_fetch) > 0:
            print_progress(
                len(timestamps_to_fetch),
                len(timestamps_to_fetch),
                f"Resolving {len(timestamps_to_fetch)} block numbers",
            )

        return [block_idx[timestamp] for timestamp in block_timestamps]

    def get_blocks(self, numbers: int | List[int]) -> List[Block]:
        """
        Get blocks by numbers.

        Args:
            number: block numbers

        Returns:
            Blocks with these numbers. ``None`` if the block doesn't exist.
        """

        if not isinstance(numbers, list):
            numbers = [numbers]

        if len(numbers) == 0:
            return []

        out = []

        block_numbers = list(set(numbers))
        grid_block_numbers = []

        for b in block_numbers:
            snapped = self._snap_to_grid(b, direction="left")
            grid_block_numbers.append(snapped)
            if snapped != b:
                right = self._next_grid_block(snapped)
                if not right is None:
                    grid_block_numbers.append(right)

        grid_block_numbers = list(set(grid_block_numbers))
        cached_grid_blocks = self._blocks_repo.find(grid_block_numbers)
        grid_block_idx = {b.number: b for b in cached_grid_blocks}
        grid_block_numbers_to_fecth = []
        for num in grid_block_numbers:
            if not num in grid_block_idx:
                grid_block_numbers_to_fecth.append(num)

        for i, num in enumerate(grid_block_numbers_to_fecth):
            print_progress(
                i,
                len(numbers),
                f"Resolving {len(grid_block_numbers_to_fecth)} block numbers",
            )
            block = self._fetch_block_from_rpc_and_save(num)
            grid_block_idx[block.number] = block

        if len(grid_block_numbers_to_fecth) > 0:
            print_progress(
                len(grid_block_numbers_to_fecth),
                len(grid_block_numbers_to_fecth),
                f"Resolving {len(numbers)} block numbers",
            )

        out = []
        for num in numbers:
            left_num = self._snap_to_grid(num, direction="left")
            if left_num == num:
                out.append(grid_block_idx[num])
                continue

            right_num = self._snap_to_grid(num, direction="right")
            left = grid_block_idx[left_num]
            right = grid_block_idx[right_num]
            block = self._synthesize_block(left, right, num)
            out.append(block)

        return out

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._block_cache = {}
        self._block_grid_cache = {}
        self._blocks_repo.purge()
        self._blocks_repo.conn.commit()

    def _resolve_from_cache(self, timestamp: int) -> Block | None:
        if timestamp in self._block_cache:
            return self._block_cache[timestamp]

        left = self._blocks_repo.get_block_before_timestamp(timestamp)
        right = self._blocks_repo.get_block_after_timestamp(timestamp)
        if left is None or right is None:
            return None
        if right.number - left.number == self.block_grid_step:
            return self._synthesize_block_from_timestamp(left, right, timestamp)
        return None

    def _synthesize_block(
        self,
        left_block: Block,
        right_block: Block,
        number: int,
    ) -> Block:
        if left_block == right_block:
            return Block(self.chain_id, left_block.number, left_block.timestamp)
        if number == left_block.number:
            return left_block
        if number == right_block.number:
            return right_block
        w = (number - left_block.number) / (right_block.number - left_block.number)
        timestamp = int((1 - w) * left_block.timestamp + w * right_block.timestamp)
        block = Block(self.chain_id, number, timestamp)
        self._block_cache[timestamp] = block
        return block

    def _synthesize_block_from_timestamp(
        self,
        left_block: Block,
        right_block: Block,
        timestamp: int,
    ) -> Block:
        if left_block == right_block:
            return Block(self.chain_id, left_block.number, left_block.timestamp)
        w = (timestamp - left_block.timestamp) / (
            right_block.timestamp - left_block.timestamp
        )
        number = int((1 - w) * left_block.number + w * right_block.number)
        prev = self._synthesize_block(left_block, right_block, number)
        nxt = self._synthesize_block(left_block, right_block, number + 1)
        if nxt.timestamp > timestamp:
            return prev
        else:
            return nxt

    def _snap_to_grid(
        self, block_number: int, direction=Literal["left"] | Literal["right"]
    ) -> int:
        if block_number >= self.latest_block.number:
            return block_number
        if block_number <= 1:
            # this doesn't fit in the logic below because of the small grid_step
            if block_number == 1 and self.block_grid_step == 1 and direction == "right":
                return 2
            return 1
        mod = block_number % self.block_grid_step
        snapped = block_number
        if mod != 0:
            if direction == "left":
                snapped = block_number - mod
            else:
                snapped = block_number - mod + self.block_grid_step

        # the lower bound for the grid is block 1
        if snapped < 1:
            return 1
        # the upper bound for the grid is the latest block
        if snapped > self.latest_block.number:
            return self.latest_block.number
        return snapped

    def _next_grid_block(self, num: int) -> int | None:
        if num == self.latest_block.number:
            return None
        if self.block_grid_step == 1:
            return num + 1

        return self._snap_to_grid(num + 1, direction="right")

    def _get_grid_block(self, number: int | None) -> Block:
        if number is None:
            return self.latest_block
        snapped_num = self._snap_to_grid(number, direction="left")
        if number != snapped_num:
            raise ValueError("API call for blocks out of grid are prohibited")

        if number in self._block_grid_cache:
            return self._block_grid_cache[number]

        block = next(iter(self._blocks_repo.find(number)), None)
        if not block is None:
            self._block_grid_cache[number] = block
            return block

        return self._fetch_block_from_rpc_and_save(number)

    def _fetch_block_from_rpc_and_save(self, number: int) -> Block | None:
        block = self._fetch_block_from_rpc(number)
        if block is None:
            if number < 1:
                block = self._fetch_block_from_rpc(1)
            else:
                block = self.latest_block
        self._blocks_repo.save([block])
        self._blocks_repo.conn.commit()
        self._block_grid_cache[block.number] = block

        return block

    def _fetch_block_from_rpc(self, number: int | None = None) -> Block | None:
        block_id = "latest" if number is None else number
        try:
            raw_block = self.w3.eth.get_block(block_id)
            block = json.loads(json_response(raw_block))
            return Block.from_dict({"chainId": self.chain_id, **block})

        except BlockNotFound:
            return None

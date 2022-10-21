from __future__ import annotations
from datetime import datetime

import json
from typing import Dict, List, Literal, Tuple
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
    _chain_id: int | None
    _grid_step: int
    _latest_block: Block | None
    _block_cache: Dict[int, Block]

    def __init__(
        self,
        blocks_repo: BlocksRepo,
        w3: Web3,
        grid_step: int,
    ):
        self._blocks_repo = blocks_repo
        self._w3 = w3
        self._grid_step = grid_step
        self._latest_block = None
        self._block_cache = {}
        self._chain_id = None

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

    @property
    def latest_block(self) -> Block:
        """
        Latest block from Ethereum (this value is cached on the first call)
        """
        if self._latest_block is None:
            self._latest_block = self._fetch_block_from_rpc()
        return self._latest_block

    @property
    def chain_id(self) -> int:
        """
        Ethereum chain_id
        """
        if self._chain_id is None:
            self._chain_id = self._w3.eth.chain_id
        return self._chain_id

    def get_latest_block_at_timestamp(self, timestamp: int) -> Block | None:
        """
        Get the first block after a timestamp.

        Args:
            timestamp: UNIX timestamp, UTC+0

        Returns:
            First block after timestamp, ``None`` if the block doesn't exist
        """
        left_block = self.get_blocks([1])[0]
        right_block = self.latest_block
        if timestamp >= right_block.timestamp:
            return self.latest_block
        if timestamp < left_block.timestamp:
            return None

        # invariant: left_block.timestamp <= timestamp < right_block.timestamp
        while right_block.number - left_block.number > self._grid_step:
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

    def get_blocks_by_timestamps(self, block_timestamps: List[int]) -> List[Block]:
        if len(block_timestamps) == 0:
            return []

        out = []
        for i, ts in enumerate(block_timestamps):
            if len(block_timestamps) > 5:
                print_progress(
                    i,
                    len(block_timestamps),
                    f"Resolving {len(block_timestamps)} block numbers",
                )
            block = self.get_latest_block_at_timestamp(ts)
            out.append(block)

        if len(block_timestamps) > 5:
            print_progress(
                len(block_timestamps),
                len(block_timestamps),
                f"Resolving {len(block_timestamps)} block numbers",
            )
        return out

    def get_blocks(self, numbers: int | List[int]) -> List[Block]:
        """
        Get block with a specific number.

        Args:
            number: block number. If ``None``, fetches the latest block

        Returns:
            Block with this number. ``None`` if the block doesn't exist.
        """

        if not isinstance(numbers, list):
            numbers = [numbers]

        out = []
        for i, num in enumerate(numbers):
            if len(numbers) > 5:
                print_progress(
                    i,
                    len(numbers),
                    f"Resolving {len(numbers)} block numbers",
                )
            left_num = self._snap_to_grid(num, direction="left")
            right_num = self._snap_to_grid(num, direction="right")
            left = self._get_grid_block(left_num)
            right = self._get_grid_block(right_num)
            out.append(self._synthesize_block(left, right, num))

        if len(numbers) > 5:
            print_progress(
                len(numbers),
                len(numbers),
                f"Resolving {len(numbers)} block numbers",
            )
        return out

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._blocks_repo.purge()
        self._blocks_repo.commit()

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

        return Block(self.chain_id, number, timestamp)

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
        next = self._synthesize_block(left_block, right_block, number + 1)
        if next.timestamp > timestamp:
            return prev
        else:
            return next

    def _snap_to_grid(
        self, block_number: int, direction=Literal["left"] | Literal["right"]
    ) -> int:
        if block_number >= self.latest_block.number:
            return block_number
        if block_number <= 1:
            # this doesn't fit in the logic below because of the small grid_step
            if block_number == 1 and self._grid_step == 1 and direction == "right":
                return 2
            return 1
        mod = block_number % self._grid_step
        snapped = block_number
        if mod != 0:
            if direction == "left":
                snapped = block_number - mod
            else:
                snapped = block_number - mod + self._grid_step

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
        if self._grid_step == 1:
            return num + 1

        return self._snap_to_grid(num + 1, direction="right")

    def _get_grid_block(self, number: int | None):
        if number is None:
            return self.latest_block
        snapped_num = self._snap_to_grid(number, direction="left")
        if number != snapped_num:
            raise ValueError("API call for blocks out of grid are prohibited")

        if number in self._block_cache:
            return self._block_cache[number]

        block = next(iter(self._blocks_repo.find(self.chain_id, number)), None)
        if not block is None:
            self._block_cache[number] = block
            return block

        block = self._fetch_block_from_rpc(number)
        if block is None:
            if number < 1:
                block = self._fetch_block_from_rpc(1)
            else:
                block = self.latest_block
        self._blocks_repo.save([block])
        self._blocks_repo.commit()
        self._block_cache[block.number] = block

        return block

    def _fetch_block_from_rpc(self, number: int | None = None) -> Block | None:
        block_id = "latest" if number is None else number
        try:
            raw_block = self._w3.eth.get_block(block_id)
            block = json.loads(json_response(raw_block))
            return Block.from_dict({"chainId": self.chain_id, **block})

        except BlockNotFound:
            return None

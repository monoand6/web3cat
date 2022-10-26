"""
Module for fetching and caching blocks metadata from web3.

The main class of this module is :class:`BlocksService`.
It is used for retrieving block metadata from web3 and storing it in
cache so that subsequent requests are returned from cache.

Example:
    ::
        from datetime import datetime
        from web3cat.fetcher.blocks import BlocksService

        service = BlocksService.create()
        latest_block = service.get_latest_block()

        new_year = datetime(2022, 1, 1, tzinfo=timezone.utc)
        new_year_block = service.get_latest_blocks_by_timestamps(int(new_year.timestamp()))[0]
        # => block 13916166

        new_year_block = service.get_latest_blocks_by_timestamps(int(new_year.timestamp()))[0]
        # cached result

        new_year_block = service.get_block(new_year_block.number)
        # cached result
"""

from fetcher.blocks.block import Block
from fetcher.blocks.repo import BlocksRepo
from fetcher.blocks.service import BlocksService

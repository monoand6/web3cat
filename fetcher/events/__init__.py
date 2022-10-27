# pylint: disable=line-too-long

"""
Module for fetching and caching events from web3.

The main class of this module is :class:`EventsService`.
It is used for retrieving events from web3 and storing them in
cache so that subsequent requests are returned from cache.

It also supports quick incremental fetches. For example,
if you fetched ERC20 Transfer events from block 10_000 to
block 20_000, then a subsequent request from block 15_000 to
block 21_000 will return 15_000 - 20_000 from cache and fetch
only 20_000 - 21_000 from web3 and save to cache.
A subsequent request for blocks 15_000 - 21_000 will read
everything from cache.

Additionally, it supports nesting of the argument filters.
Imagine you

1. Fetched all Transfer events from blocks 2000 - 4000
2. Fetched Transfer events with filter :code:`{"from": "0x1234..."}` for blocks 4000 - 6000
3. Fetched Transfer events with filter :code:`{"to": "0x5678..."}` for blocks 6000 - 8000

Now when you query events with filter :code:`{"from": "0x1234...", "to": "0x5678..."}`
for blocks 2000 - 8000 :class:`EventsService` is smart enough
to figure out that all events are already in cache and
serve the cached result.

Example:
    ::

        from web3.auto import w3
        from web3cat.fetcher.events import EventsService

        dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        dai_abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]
        token = w3.eth.contract(address=dai_address, abi=dai_abi)

        service = EventsService.create()
        events = service.get_events(
            token.events.Transfer, from_block=15830000, to_block=15840100
        )
"""

from fetcher.events.event import Event
from fetcher.events.repo import EventsRepo
from fetcher.events.service import EventsService

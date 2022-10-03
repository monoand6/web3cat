"""
Module with repos and models for :class:`fetcher.events_indices.index.EventsIndex`.

:class:`EventsIndex` stores 
the information about fetched events.
The chain_id, address of the contract, event name and arguments for
lookup uniquely defines an instance of :class:`EventsIndex` 
and thus an entry in the :code:`events_indices` table in the cache.

Then each entry stores the blocks with already fetched events.

Note:
    Index has some granularity of storing the blocks defined by the
    :const:`BLOCKS_PER_BIT` constant. It has several implications:

    1. You cannot fetch events for less than :const:`BLOCKS_PER_BIT` blocks
    2. Fetch start and end blocks should be snapped to :const:`BLOCKS_PER_BIT` grid.

Example:

    In this example an instance of :class:`EventsIndex` evolves when 
    fetching the same events for different blocks.

Assume :code:`BLOCKS_PER_BIT == 1000`

**Step1: Fetch events for blocks 11337 - 13798**

First the fetch blocks are rounded to 11000 - 14000, events are fetched
for these blocks.
Start timestamp in this case should be 11000. However there's the rule
that start timestamp must be a multiple of 8 * :const:`BLOCKS_PER_BIT`.
So it's rounded down to 8000.

Events index is updated to :code:`00001F401C`
00011100

The structure of the events index:

+-----------------+------------+-----------------+---------------+
| Timestamp (hex) | Mask (hex) | Timestamp (dec) | Mask (binary) |
+-----------------+------------+-----------------+---------------+
| 00001F40        | 1C         | 8000            | 00011100      |
+-----------------+------------+-----------------+---------------+

So the first bit of the mask is blocks 8000 - 9000, the second is 9000 - 10000, etc.

**Step2: Fetch blocks 15546 - 19403**

Round blocks to 15000 - 20000, update :class:`EventsIndex` to :code:`00001F401DF0`

+-----------------+------------+-----------------+------------------+
| Timestamp (hex) | Mask (hex) | Timestamp (dec) | Mask (binary)    |
+-----------------+------------+-----------------+------------------+
| 00002AF8        | 1DF0       | 8000            | 0001110111110000 |
+-----------------+------------+-----------------+------------------+

**Step3: Fetch blocks 3000 - 5000**

Now the start timestamp will be updated to 0 (the multiple of 8 * :const:`BLOCKS_PER_BIT`).
The new index is :code:`000000001C1DF0`

+-----------------+------------+-----------------+--------------------------+
| Timestamp (hex) | Mask (hex) | Timestamp (dec) | Mask (binary)            |
+-----------------+------------+-----------------+--------------------------+
| 00000000        | 1C1DF0     | 0               | 000111000001110111110000 |
+-----------------+------------+-----------------+--------------------------+

"""

from fetcher.events_indices.index import EventsIndex
from fetcher.events_indices.index_data import EventsIndexData, BLOCKS_PER_BIT
from fetcher.events_indices.bitarray import BitArray
from fetcher.events_indices.repo import EventsIndicesRepo

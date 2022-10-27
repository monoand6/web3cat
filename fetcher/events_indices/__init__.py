"""
Module with repos and models for :class:`EventsIndex`.

:class:`EventsIndex` stores the information about fetched events.
Each :class:`EventsIndex` is persisted in the :code:`events_indices` table.
:class:`EventsIndex` has a field ``data`` that stores the blocks
for which events (with given chain_id, contract address,
event name, and arguments for lookup) were already fetched.

Note:
    :class:`EventsIndex` has some granularity in storing the blocks defined by the
    :const:`constants.BLOCKS_PER_BIT` constant. This means:

    1. You cannot fetch events for less than :const:`constants.BLOCKS_PER_BIT` blocks
    2. Fetch start and end blocks should be snapped to the :const:`constants.BLOCKS_PER_BIT` grid.

Example
~~~~~~~

As an illustratory example, consider how an instance of :class:`EventsIndex`
evolves when fetching the same type of events for different blocks.

Assume :code:`BLOCKS_PER_BIT == 1000`.

**Step1: Fetch events for blocks 11337 - 13798**

First, the fetch blocks are rounded to 11000 - 14000, and events are fetched
for these blocks.
Start timestamp, in this case, should be 11000. However, there's the rule
that start timestamp must be a multiple of 8 * :const:`constants.BLOCKS_PER_BIT`
(for performance reasons). So it's rounded down to 8000.

:class:`EventsIndex`'s ``data`` is updated to ``00001F401C``

+-----------------+------------+-----------------+---------------+
| Timestamp (hex) | Mask (hex) | Timestamp (dec) | Mask (bin)    |
+=================+============+=================+===============+
| 00001F40        | 1C         | 8000            | 00011100      |
+-----------------+------------+-----------------+---------------+

So the first bit of the mask is blocks 8000 - 9000, the second is 9000 - 10000, etc.

**Step2: Fetch blocks 15546 - 19403**

Round blocks to 15000 - 20000, update ``data`` to ``00001F401DF0``

+-----------------+------------+-----------------+------------------+
| Timestamp (hex) | Mask (hex) | Timestamp (dec) | Mask (bin)       |
+=================+============+=================+==================+
| 00001F40        | 1DF0       | 8000            | 0001110111110000 |
+-----------------+------------+-----------------+------------------+

**Step3: Fetch blocks 3000 - 5000**

Now the start timestamp will be updated to 0 (the multiple of 8 * :const:`constants.BLOCKS_PER_BIT`,
rounded down). The new index is ``000000001C1DF0``

+-----------------+------------+-----------------+--------------------------+
| Timestamp (hex) | Mask (hex) | Timestamp (dec) | Mask (bin)               |
+=================+============+=================+==========================+
| 00000000        | 1C1DF0     | 0               | 000111000001110111110000 |
+-----------------+------------+-----------------+--------------------------+

"""

from fetcher.events_indices.index import EventsIndex
from fetcher.events_indices.index_data import EventsIndexData
from fetcher.events_indices.bitarray import BitArray
from fetcher.events_indices.repo import EventsIndicesRepo

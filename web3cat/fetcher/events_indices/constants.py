BLOCKS_PER_BIT = 1_000
"""
Defines the granularity of event indices. Blocks are divided into chunks.
Each chunk has the size of :const:`BLOCKS_PER_BIT`. If the bit is set,
the events for the whole chunk are fetched. Partial fetches are not
permitted.
"""

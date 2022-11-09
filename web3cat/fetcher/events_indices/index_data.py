from __future__ import annotations
import json
from typing import Any, Dict
from web3cat.fetcher.events_indices.constants import BLOCKS_PER_BIT
from web3cat.fetcher.events_indices.bitarray import BitArray


class EventsIndexData:
    """
    Stores blocks numbers with already fetched events.

    All blocks are divided by chunks of
    :const:`constants.BLOCKS_PER_BIT` size. A bit set to ``True``
    means that events for the chunk were already fetched.

    As a storage optimistaion :code:`start_block` parameter is used.
    It is the first block that has ``True`` entries after it.
    Obviously, it must be a multiple of :const:`constants.BLOCKS_PER_BIT`.
    However, there's an additional restriction of :code:`start_block` being
    a multiple of 8 * :const:`constants.BLOCKS_PER_BIT` (for performance
    reasons).

    See the example section in :mod:`fetcher.events_indices` for
    more details.

    Args:
        start_block: The starting block for the index
        mask: A bitset for storing index values
    """

    _start_block: int | None
    end_block: int | None
    _mask: BitArray
    _blocks_per_bit: int

    def __init__(
        self,
        start_block: int | None = None,
        end_block: int | None = None,
        mask: bytes | None = None,
    ):
        self._blocks_per_bit = BLOCKS_PER_BIT
        self._start_block = None
        self.end_block = end_block
        self._mask = BitArray(mask or [])
        self._update_start_block(start_block)

    def set_range(self, start_block: int, end_block: int, value: bool):
        """
        Set blocks range from :code:`start_block` (inclusive)
        to :code:`end_block` (non-inclusive) to ``value``.

        Args:
            start_block: start of the range (inclusive)
            end_block: end of the range (non-inclusive)

        Exceptions:
            The :code:`start_block` and :code:`end_block` arguments
            must be a multiples of :const:`constants.BLOCKS_PER_BIT`
            (:meth:`snap_block_to_grid` can be used for that).
            Otherwise :class:`IndexError` is raised.
        """
        if (
            start_block % self._blocks_per_bit != 0
            or end_block % self._blocks_per_bit != 0
        ):
            raise IndexError(
                f"EventsIndexData only multiples of `{self._blocks_per_bit}` "
                f"are allowed for start_block and end_block. Got `{start_block}` "
                f"and `{end_block}`"
            )
        self._update_start_block(start_block)
        start_idx = self._block_to_bit(start_block)
        end_idx = self._block_to_bit(end_block)
        self._mask.set_range(start_idx, end_idx, value)

    def snap_block_to_grid(self, block: int) -> int:
        """
        Round down block to the nearest chunk start
        (chunks of size :const:`constants.BLOCKS_PER_BIT`)
        """
        return block - block % self._blocks_per_bit

    def step(self) -> int:
        """
        Returns :const:`constants.BLOCKS_PER_BIT`.
        """
        return self._blocks_per_bit

    def dump(self) -> bytes:
        """
        Serialize class data into binary.

        The binary format is

        +-------------+-----------+---------------------+
        | start_block | end_block | mask                |
        +=============+===========+=====================+
        | 4 bytes     | 4 bytes   | n bytes (as needed) |
        +-------------+-----------+---------------------+
        """
        if self._start_block is None:
            return bytes()
        bytes4 = self._start_block.to_bytes(4, "big")
        end_block = self.end_block or 0
        bytes48 = end_block.to_bytes(4, "big")
        return bytes4 + bytes48 + self._mask._data  # pylint: disable=protected-access

    @staticmethod
    def load(data: bytes) -> EventsIndexData:
        """
        Restore class from binary. See :meth:`dump` for binary format.

        Returns:
            An instance of :class:`EventsIndexData`
        """
        if len(data) < 4:
            return EventsIndexData()
        start_block = int.from_bytes(data[0:4], "big")
        end_block = int.from_bytes(data[4:8], "big")
        if end_block == 0:
            end_block = None
        mask = data[8:]
        return EventsIndexData(start_block, end_block, mask)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert :class:`EventsIndexData` to dict
        """
        return {
            "startBlock": self._start_block,
            "endBlock": self.end_block,
            "mask": self._mask.to_hex(),
        }

    def __getitem__(self, block: int) -> bool:
        """
        Checks if the current block was fetched or not

        Args:
            block: block number

        Returns:
            ``True`` if the block was fetched, ``False`` otherwise
        """
        if self._start_block is None:
            return False
        if not self.end_block is None:
            if block >= self.end_block:
                return False
        bit = self._block_to_bit(block)
        if bit < 0 or bit >= len(self._mask):
            return False
        return self._mask[bit]

    def __repr__(self) -> str:
        return f"EventsIndexData({json.dumps(self.to_dict())})"

    def _update_start_block(self, block: int | None):
        """

        Example

        Before:
        start_block: 0x10
        mask: 0011000011111100
        0               1
        0123456789abcdef0123456789abcdef
                        0011000011111100

        update start block to 0x04
        prepend 2 bytes, update start_block to 0x00 (multiple of 8 bits)

        After:
        start_block: 0x00
        mask: 00000000000000000011000011111100
        0               1
        0123456789abcdef0123456789abcdef
        00000000000000000011000011111100

        """
        if block is None:
            return

        bit_number = block // self._blocks_per_bit
        byte_number = bit_number // 8
        # snap block to grid of bytes and blocks_per_bit
        block = byte_number * 8 * self._blocks_per_bit

        if self._start_block is None:
            self._start_block = block
            return

        if block >= self._start_block:
            # no need to update
            return

        num_bytes_to_prepend = (
            (self._start_block - block) // self._blocks_per_bit
        ) // 8
        self._mask.prepend_empty_bytes(num_bytes_to_prepend)
        self._start_block = block

    def _block_to_bit(self, block: int) -> int:
        return (block - self._start_block) // self._blocks_per_bit

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

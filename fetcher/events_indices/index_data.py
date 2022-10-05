from __future__ import annotations
import json
from typing import Any, Dict, Tuple
from fetcher.events_indices.constants import BLOCKS_PER_BIT
from fetcher.events_indices.bitarray import BitArray


class EventsIndexData:
    _start_block: int | None
    _mask: BitArray
    _blocks_per_bit: int

    def __init__(
        self,
        start_block: int | None = None,
        mask: bytes | None = None,
    ):
        self._blocks_per_bit = BLOCKS_PER_BIT
        self._start_block = None
        self._mask = BitArray(mask or [])
        self._update_start_block(start_block)

    def set_range(self, start_block: int, end_block: int, value: bool):
        if (
            start_block % self._blocks_per_bit != 0
            or end_block % self._blocks_per_bit != 0
        ):
            raise IndexError(
                f"EventsIndexData only multiples of `{self._blocks_per_bit}` are allowed for start_block and end_block. Got `{start_block}` and `{end_block}`"
            )
        self._update_start_block(start_block)
        start_idx = self._block_to_bit(start_block)
        end_idx = self._block_to_bit(end_block)
        self._mask.set_range(start_idx, end_idx, value)

    def snap_block_to_grid(self, block: int) -> int:
        return block - block % self._blocks_per_bit

    def step(self) -> int:
        return self._blocks_per_bit

    def dump(self) -> bytes:
        if self._start_block is None:
            return bytes()
        bytes4 = self._start_block.to_bytes(4, "big")
        return bytes4 + self._mask._data

    def load(data: bytes) -> EventsIndexData:
        if len(data) < 4:
            return EventsIndexData()
        block = int.from_bytes(data[0:4], "big")
        mask = data[4:]
        return EventsIndexData(block, mask)

    def __getitem__(self, block: int) -> bool:
        if self._start_block is None:
            return False
        bit = self._block_to_bit(block)
        if bit < 0 or bit >= len(self._mask):
            return False
        return self._mask[bit]

    def __repr__(self) -> str:
        return f"EventsIndexData(start_block: {self._start_block}, mask: {self._mask})"

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

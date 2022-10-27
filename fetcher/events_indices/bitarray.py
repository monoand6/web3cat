from hexbytes import HexBytes


class BitArray:
    """
    This is a simple bitset tailored for :mod:`events_indices` needs.

    Example:

        >>> bits = BitArray()
        >>> bits[3]
        False
        >>> bits[3] = True # 00010000
        >>> bits[0]
        False
        >>> bits[3]
        True
        >>> bits.prepend_empty_bytes(2) # 000000000000000000010000
        >>> bits[0]
        False
        >>> bits[3]
        False
        >>> bits[2 * 8 + 3]
        True
        >>> bits.set_range(0, 4, True) # 111100000000000000010000
        >>> bits[0]
        True
        >>> bits[1]
        True
        >>> bits[2]
        True
        >>> bits[3]
        True
        >>> bits[4]
        False

    """

    _data: bytearray

    def __init__(self, data: bytes | None = None):
        self._data = bytearray(data or [])

    def __getitem__(self, pos: int) -> bool:
        """
        Get the bit value at ``pos``

        Args:
            pos: The position of the bit

        Returns:
            Bit value (``False`` or ``True``)
        """
        byte_idx = pos // 8
        if byte_idx >= len(self._data):
            return False
        byte = self._data[pos // 8]
        return byte & (1 << (pos % 8)) != 0

    def __setitem__(self, pos: int, value: bool):
        """
        Set the bit value at ``pos``

        Args:
            pos: The position of the bit
            value: Bit value (``False`` or ``True``)
        """
        self._ensure_length(pos)
        if value:
            self._data[pos // 8] |= 1 << (pos % 8)
        else:
            self._data[pos // 8] &= 255 - (1 << (pos % 8))

    def __len__(self):
        return len(self._data) * 8

    def prepend_empty_bytes(self, num_bytes: int):
        """
        Adds zero bytes (8 * num_bytes ``False`` values) before the bitarray.

        Args:
            num_bytes: Number of bytes to add
        """
        new_data = bytearray([0] * num_bytes)
        for b in self._data:
            new_data.append(b)
        self._data = new_data

    def set_range(self, start: int, end: int, value: bool):
        """
        Set value for bits starting at ``start`` (inclusive) and ending
        at ``end`` (non-inclusive).

        Args:
            start: start of the range (inclusive)
            end: end of the range (non-inclusive)

        Exceptions:
            Both values should be >= 0 and ``end`` >= ``start``.
            Otherwise :class:`IndexError` is raised.

        """
        if start > end:
            raise IndexError(f"BitArray set_range with start: {start}, end: {end}")
        if end < 0:
            raise IndexError(f"BitArray set_range with start: {start}, end: {end}")
        self._ensure_length(end - 1)

        # inside the common byte or in neighbor bytes
        if end - start < 8:
            for i in range(start, end):
                self[i] = value
            return

        # at least in neighbor bytes
        full_start = start // 8 + 1
        full_end = end // 8
        val = 0 if value is False else 255
        for i in range(full_start, full_end):
            self._data[i] = val
        for i in range(start, full_start * 8):
            self[i] = value
        for i in range(full_end * 8, end):
            self[i] = value

    def to_hex(self) -> str:
        """
        Returns hex representation starting with '0x'.

        Example:

            >>> b = BitArray()
            >>> b.to_hex()
            '0x'
            >>> b[3] = True
            >>> b.to_hex()
            '0x08'
            >>> b[2] = True
            >>> b.to_hex()
            '0x0c'
            >>> b[4] = True
            >>> b.to_hex()
            '0x1c'
            >>> b[8] = True
            >>> b.to_hex()
            '0x1c01'

        """
        return HexBytes(self._data).hex()

    def _ensure_length(self, idx: int):
        while len(self._data) <= idx // 8:
            self._data.append(0)

    def __repr__(self):
        out = ""
        for b in self._data:
            for i in range(8):
                if b & (1 << i) > 0:
                    out += "1"
                else:
                    out += "0"
        return f"BitArray({out})"

    def __eq__(self, other):
        if type(other) is type(self):
            return self._data == other._data
        return False

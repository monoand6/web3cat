class BitArray:
    _data: bytearray

    def __init__(self, data: bytes | None = None):
        self._data = bytearray(data or [])

    def __getitem__(self, idx: int) -> bool:
        byte = self._data[idx // 8]
        return byte & (1 << (idx % 8)) != 0

    def __setitem__(self, idx: int, value: bool):
        self._ensure_length(idx)
        if value:
            self._data[idx // 8] |= 1 << (idx % 8)
        else:
            self._data[idx // 8] &= 255 - (1 << (idx % 8))

    def __len__(self):
        return len(self._data) * 8

    def prepend_empty_bytes(self, num_bytes: int):
        new_data = bytearray([0] * num_bytes)
        for b in self._data:
            new_data.append(b)
        self._data = new_data

    def set_range(self, start: int, end: int, value: bool):
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
        val = 0 if value == False else 255
        for i in range(full_start, full_end):
            self._data[i] = val
        for i in range(start, full_start * 8):
            self[i] = value
        for i in range(full_end * 8, end):
            self[i] = value

    def _ensure_length(self, idx: int):
        while len(self._data) <= idx // 8:
            self._data.append(0)

    def __repr__(self):
        res = ""
        for b in self._data:
            for i in range(8):
                if b & (1 << i) > 0:
                    res += "1"
                else:
                    res += "0"
        return f"BitArray({res})"

    def __eq__(self, other):
        if type(other) is type(self):
            return self._data == other._data
        return False

SECONDS_IN_BIT = 86400


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


class EventsIndex:
    _start_timestamp: int | None
    _mask: BitArray

    def __init__(self, start_timestamp: int | None = None, mask: bytes | None = None):
        self._update_timestamp(start_timestamp)
        self._mask = BitArray(mask or [])

    def set_range(self, start_timestamp: int, end_timestamp: int, value: bool):
        self._update_timestamp(start_timestamp)
        start_idx = self._timestamp_to_idx(start_timestamp)
        end_idx = self._timestamp_to_idx(end_timestamp)
        self._mask.set_range(start_idx, end_idx, value)

    def __getitem__(self, timestamp: int) -> bool:
        if not self._start_timestamp:
            return False
        idx = self._timestamp_to_idx(timestamp)
        if idx < 0 or idx >= len(self._mask):
            return False
        return self._mask[idx]

    def _update_timestamp(self, timestamp: int | None):
        if not timestamp:
            return

        timestamp -= timestamp % SECONDS_IN_BIT

        if not self._start_timestamp:
            self._start_timestamp = timestamp
            return

        if timestamp >= self._start_timestamp:
            return

        # make sure that offset % 8 would be == 0, so that we can add bytes
        while (timestamp - self._start_timestamp) // SECONDS_IN_BIT % 8 != 0:
            timestamp -= SECONDS_IN_BIT

        self._start_timestamp = timestamp
        num_bytes = (timestamp - self._start_timestamp) // SECONDS_IN_BIT // 8
        self._mask.prepend_empty_bytes(num_bytes)

    def _timestamp_to_idx(self, timestamp: int) -> int:
        return (timestamp - self._start_timestamp) // SECONDS_IN_BIT

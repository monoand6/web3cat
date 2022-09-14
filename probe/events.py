SECONDS_IN_BIT = 86400


class BitArray:
    _data: bytearray

    def __init__(self):
        self._data = bytearray()

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

    def set_range(self, start: int, end: int, value: bool):
        if start >= end:
            return
        self._ensure_length(end)

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
    _mask: bytes | None

    def __init__(self, start_timestamp: int | None = None, mask: bytes | None = None):
        self._start_timestamp -= start_timestamp % SECONDS_IN_BIT
        self._mask = mask

    def set_range(self, start_timestamp: int, end_timestamp: int):
        start_timestamp -= start_timestamp % SECONDS_IN_BIT
        end_timestamp -= end_timestamp % SECONDS_IN_BIT
        end_timestamp += SECONDS_IN_BIT
        if not self._start_timestamp:
            self._start_timestamp = start_timestamp
        start_idx = self._timestamp_to_idx(start_timestamp)

    def __getitem__(self, timestamp: int) -> bool:
        if timestamp < self._start_timestamp:
            return False

        return self._get_bit_entry(self._timestamp_to_idx(timestamp))

    def _get_bit_entry(self, idx: int) -> bool:
        if idx < 0 or idx >= len(self._mask) * 8:
            return False
        byte = self._mask[idx // 8]
        return byte & (1 << (idx % 8)) != 0

    def _set_bit_entry(self, idx: int) -> bool:
        if idx < 0 or idx >= len(self._mask) * 8:
            raise IndexError(
                f"Out of range for EventsIndex. Tried to access index `{idx}` but mask length is `{len(self._mask) * 8}`"
            )
        self._mask[idx // 8] |= 1 << (idx % 8)

    def _timestamp_to_idx(self, timestamp: int) -> int:
        return (timestamp - self._start_timestamp) // SECONDS_IN_BIT

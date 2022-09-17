from probe.events.bitarray import BitArray

SECONDS_IN_BIT = 86400
FIRST_EVM_TIMESTAMP = 1438269000


class EventsIndex:
    _start_timestamp: int | None
    _mask: BitArray

    def __init__(self, start_timestamp: int | None = None, mask: bytes | None = None):
        self._start_timestamp = None
        self._mask = BitArray(mask or [])
        self._update_timestamp(start_timestamp)

    def set_range(self, start_timestamp: int, end_timestamp: int, value: bool):
        if start_timestamp % SECONDS_IN_BIT != 0 or end_timestamp % SECONDS_IN_BIT != 0:
            raise IndexError(
                f"EventsIndex only multiples of {SECONDS_IN_BIT} are allowed for start_timestamp and end_timestamp. Got {start_timestamp} and {end_timestamp}"
            )
        self._update_timestamp(start_timestamp)
        start_idx = self._timestamp_to_idx(start_timestamp)
        end_idx = self._timestamp_to_idx(end_timestamp)
        self._mask.set_range(start_idx, end_idx, value)

    def snap_to_grid(self, timestamp: int) -> int:
        return timestamp - timestamp % SECONDS_IN_BIT

    def __getitem__(self, timestamp: int) -> bool:
        if self._start_timestamp is None:
            return False
        idx = self._timestamp_to_idx(timestamp)
        if idx < 0 or idx >= len(self._mask):
            return False
        return self._mask[idx]

    def _update_timestamp(self, timestamp: int | None):
        if timestamp is None:
            return

        timestamp -= timestamp % SECONDS_IN_BIT

        if self._start_timestamp is None:
            self._start_timestamp = timestamp
            return

        if timestamp >= self._start_timestamp:
            return

        # make sure that offset % 8 would be == 0, so that we can add bytes
        while ((self._start_timestamp - timestamp) // SECONDS_IN_BIT) % 8 != 0:
            timestamp -= SECONDS_IN_BIT

        num_bytes = ((self._start_timestamp - timestamp) // SECONDS_IN_BIT) // 8
        self._start_timestamp = timestamp
        self._mask.prepend_empty_bytes(num_bytes)

    def _timestamp_to_idx(self, timestamp: int) -> int:
        return (timestamp - self._start_timestamp) // SECONDS_IN_BIT

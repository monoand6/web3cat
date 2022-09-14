from typing import List
from hypothesis import given
from hypothesis.strategies import lists, integers, tuples
from probe.events import BitArray


@given(lists(integers(0, 1000), min_size=1, max_size=100))
def test_bit_array_set_item(ints: List[int]):
    b = BitArray()
    for i in range(len(ints)):
        b[ints[i]] = True
    for i in range(0, len(b)):
        assert b[i] == (i in ints)


@given(
    integers(0, 1000),
    integers(0, 1000),
    lists(integers(0, 1000), min_size=1, max_size=100),
)
def test_bit_array_set_range(min: int, max: int, ints: List[int]):
    if max < min:
        min, max = max, min
    b = BitArray()
    for i in range(len(ints)):
        b[ints[i]] = True
    b.set_range(min, max, True)
    for i in range(0, len(b)):
        assert b[i] == (i in ints) or ((i >= min) and (i < max))

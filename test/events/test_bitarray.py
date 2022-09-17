from typing import List
import pytest
from hypothesis import given
from hypothesis.strategies import lists, integers, tuples
from probe.events.bitarray import BitArray


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


@given(
    integers(0, 1000),
    lists(integers(0, 255), min_size=1, max_size=100),
)
def test_bit_array_preprend_empty_bytes(num_bytes: int, init_bytes: List[int]):
    arr = BitArray(bytes(init_bytes))
    arr2 = BitArray(bytes(init_bytes))
    arr2.prepend_empty_bytes(num_bytes)
    for i in range(0, num_bytes * 8):
        assert not arr2[i]
    for i in range(0, len(arr)):
        assert arr[i] == arr2[i + num_bytes * 8]


def test_bit_array_set_range_edge_1():
    b = BitArray()
    with pytest.raises(IndexError):
        b.set_range(-2, -1, True)

    with pytest.raises(IndexError):
        b.set_range(0, -1, True)

    with pytest.raises(IndexError):
        b.set_range(1, 0, True)

    b.set_range(0, 0, True)
    assert len(b) == 0

    b.set_range(0, 1, True)
    assert len(b) == 8
    assert b[0] == True
    assert b[1] == False

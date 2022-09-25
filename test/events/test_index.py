import pytest
from probe.events.index import EventsIndexData, SECONDS_IN_BIT
from hypothesis import given
from hypothesis.strategies import lists, integers, tuples


START = 1538269000 - 1538269000 % 86400


@given(lists(integers(8, 100)), lists(integers(8, 100)))
def test_set_range(begins: list[int], ends: list[int]):
    index = EventsIndexData()
    test_index = {}
    N = min(len(begins), len(ends))
    for i in range(N):
        begin, end = min(begins[i], ends[i]), max(begins[i], ends[i])
        for x in range(begin, end):
            test_index[x] = True
        index.set_range(begin * 86400, end * 86400, True)
    for i in range(0, 100):
        assert index[i * 86400] == (i in test_index)


@given(lists(integers(8, 100)), lists(integers(8, 100)))
def test_dump_load(begins: list[int], ends: list[int]):
    index = EventsIndexData()
    N = min(len(begins), len(ends))
    for i in range(N):
        begin, end = min(begins[i], ends[i]), max(begins[i], ends[i])
        index.set_range(begin * 86400, end * 86400, True)
    restored = EventsIndexData.load(index.dump())
    for i in range(0, 100):
        assert index[i * 86400] == restored[i * 86400]


def test_set_range_1():
    index = EventsIndexData(START)
    index.set_range(START, START, True)
    assert not index[START]
    assert not index[START + 100]
    assert not index[START + SECONDS_IN_BIT - 1]
    assert not index[START + SECONDS_IN_BIT]


def test_set_range_2():
    index = EventsIndexData(START)
    with pytest.raises(IndexError):
        index.set_range(START + 100, START + SECONDS_IN_BIT + 101, True)


def test_set_range_3():
    index = EventsIndexData(START)
    index.set_range(START, START + SECONDS_IN_BIT, True)
    assert index[START]
    assert index[START + 101]
    assert index[START + SECONDS_IN_BIT - 1]
    assert not index[START + SECONDS_IN_BIT]


def test_set_range_4():
    index = EventsIndexData(START)
    index.set_range(START, START + 2 * SECONDS_IN_BIT, True)
    assert index[START]
    assert index[START + 101]
    assert index[START + SECONDS_IN_BIT - 1]
    assert index[START + SECONDS_IN_BIT]
    assert index[START + SECONDS_IN_BIT + 1]
    assert index[START + SECONDS_IN_BIT - 1]
    assert index[START + 2 * SECONDS_IN_BIT - 1]
    assert not index[START + 2 * SECONDS_IN_BIT]
    assert not index[START + 2 * SECONDS_IN_BIT + 1]

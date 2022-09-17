import pytest
from probe.events.index import EventsIndex, SECONDS_IN_BIT
from hypothesis import given
from hypothesis.strategies import lists, integers, tuples


START = 1538269000 - 1538269000 % 86400


def test_set_range_1():
    index = EventsIndex(START)
    index.set_range(START, START, True)
    assert not index[START]
    assert not index[START + 100]
    assert not index[START + SECONDS_IN_BIT - 1]
    assert not index[START + SECONDS_IN_BIT]


def test_set_range_2():
    index = EventsIndex(START)
    with pytest.raises(IndexError):
        index.set_range(START + 100, START + SECONDS_IN_BIT + 101, True)


def test_set_range_3():
    index = EventsIndex(START)
    index.set_range(START, START + SECONDS_IN_BIT, True)
    assert index[START]
    assert index[START + 101]
    assert index[START + SECONDS_IN_BIT - 1]
    assert not index[START + SECONDS_IN_BIT]


def test_set_range_4():
    index = EventsIndex(START)
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


@given(
    integers(START - 10_000_000, START + 10_000_000),
    integers(START - 10_000_000, START + 10_000_000),
)
def test_set_range_5(start, end):
    if end < start:
        start, end = end, start
    start -= start % SECONDS_IN_BIT
    end -= end % SECONDS_IN_BIT
    index = EventsIndex(START)
    index.set_range(START, START + 2 * SECONDS_IN_BIT, True)
    index.set_range(start, end, True)
    for i in range(START - 10_000_000, START + 10_000_000, SECONDS_IN_BIT):

        val = (i >= start and i < end) or (
            i >= START and i < START + 2 * SECONDS_IN_BIT
        )
        assert index[i] == val

import pytest
from probe.events_indices.index import EventsIndexData, EventsIndex
from hypothesis import given
from hypothesis.strategies import lists, integers, tuples
from events_indices.strategies import events_index


@given(events_index())
def test_events_index_from_to_tuple(idx: EventsIndex):
    assert EventsIndex.from_tuple(idx.to_tuple()) == idx


@given(events_index())
def test_events_index_data_dump_load(idx: EventsIndex):
    data = idx.data
    restored = EventsIndexData.load(data.dump())
    for i in range(0, 1_000_000, 10000):
        assert data[i] == restored[i]

    assert EventsIndex.from_tuple(idx.to_tuple()) == idx


@given(lists(integers(0, 1_000_000)), lists(integers(0, 1_000_000)))
def test_events_index_data_set_range(begins: list[int], ends: list[int]):
    index = EventsIndexData()
    mock_index = {}
    N = min(len(begins), len(ends))
    for i in range(N):
        begin, end = min(begins[i], ends[i]), max(begins[i], ends[i])
        begin, end = index.snap_block_to_grid(begin), index.snap_block_to_grid(end)
        for x in range(begin, end, 10000):
            mock_index[x] = True
        index.set_range(begin, end, True)
    for i in range(0, 1_000_000, 10_000):
        assert index[i] == (i in mock_index)


def test_events_index_data_set_range_1():
    index = EventsIndexData()
    index.set_range(0, 0, True)
    assert not index[0]
    assert not index[100]
    assert not index[9999]
    assert not index[10000]


def test_events_index_data_set_range_2():
    index = EventsIndexData()
    with pytest.raises(IndexError):
        index.set_range(1, 2, True)


def test_events_index_data_set_range_3():
    index = EventsIndexData()
    index.set_range(0, 10000, True)
    assert index[0]
    assert index[101]
    assert index[9999]
    assert not index[10000]


def test_events_index_data_set_range_4():
    index = EventsIndexData()
    index.set_range(10000, 20000, True)
    assert not index[0]
    assert not index[101]
    assert not index[9999]
    assert index[10000]
    assert index[10100]
    assert index[19999]
    assert not index[20000]


def test_events_index_data_set_range_5():
    index = EventsIndexData()
    index.set_range(0, 20000, True)
    assert index[0]
    assert index[101]
    assert index[9999]
    assert index[10000]
    assert index[10001]
    assert index[19999]
    assert not index[20000]
    assert not index[20001]

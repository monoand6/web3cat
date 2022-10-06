import pytest
from fetcher.events_indices.index import EventsIndexData, EventsIndex
from hypothesis import given
from hypothesis.strategies import lists, integers, tuples
from events_indices.strategies import events_index
from fetcher.events_indices.index_data import BLOCKS_PER_BIT


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


@given(
    lists(integers(0, 100 * BLOCKS_PER_BIT)), lists(integers(0, 100 * BLOCKS_PER_BIT))
)
def test_events_index_data_set_range(begins: list[int], ends: list[int]):
    index = EventsIndexData()
    mock_index = {}
    N = min(len(begins), len(ends))
    for i in range(N):
        begin, end = min(begins[i], ends[i]), max(begins[i], ends[i])
        begin, end = index.snap_block_to_grid(begin), index.snap_block_to_grid(end)
        for x in range(begin, end, index.step()):
            mock_index[x] = True
        index.set_range(begin, end, True)
    for i in range(0, 100 * index.step(), index.step()):
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
    index.set_range(0, index.step(), True)
    assert index[0]
    assert index[11]
    assert index[index.step() - 1]
    assert not index[index.step()]


def test_events_index_data_set_range_4():
    index = EventsIndexData()
    index.set_range(index.step(), 2 * index.step(), True)
    assert not index[0]
    assert not index[11]
    assert not index[index.step() - 1]
    assert index[index.step()]
    assert index[index.step() + 11]
    assert index[2 * index.step() - 1]
    assert not index[2 * index.step()]


def test_events_index_data_set_range_5():
    index = EventsIndexData()
    index.set_range(0, 2 * index.step(), True)
    assert index[0]
    assert index[11]
    assert index[index.step() - 1]
    assert index[index.step()]
    assert index[index.step() + 1]
    assert index[2 * index.step() - 1]
    assert not index[2 * index.step()]
    assert not index[2 * index.step() + 1]


def test_events_index_data_to_dict():
    index = EventsIndexData()
    index.set_range(11000, 14000, True)
    assert index.to_dict() == {"start_block": 8000, "mask": "0x38"}

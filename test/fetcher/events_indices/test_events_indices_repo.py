from typing import Any, Dict, Tuple
from hypothesis import given
from fetcher.events_indices.index import EventsIndex
from fetcher.events_indices.index_data import EventsIndexData

from fetcher.events_indices.repo import EventsIndicesRepo, is_softer_filter_than
from events_indices.strategies import args_subset_and_superset


def test_read_write(events_indices_repo: EventsIndicesRepo):
    events_index_data = EventsIndexData(100)
    chain_id = 1
    events_index = EventsIndex(
        chain_id,
        "0x1234",
        "TestEvent",
        {"from": "0x1234"},
        events_index_data,
    )
    events_indices_repo.save([events_index])
    list1 = events_indices_repo.find_indices(
        chain_id, events_index.address, events_index.event
    )
    list2 = events_indices_repo.find_indices(
        chain_id, events_index.address, events_index.event, {}
    )
    assert len(list1) == 0
    assert len(list2) == 0

    list3 = events_indices_repo.find_indices(
        chain_id, events_index.address, events_index.event, events_index.args
    )
    list4 = events_indices_repo.find_indices(
        chain_id, events_index.address, events_index.event, {"from": "0x1234"}
    )
    list5 = events_indices_repo.find_indices(
        chain_id,
        events_index.address,
        events_index.event,
        {"value": 1, "from": "0x1234"},
    )

    assert len(list3) == 1
    assert len(list4) == 1
    assert len(list5) == 1
    assert list3[0] == events_index
    assert list3[0] == list4[0]
    assert list4[0] == list5[0]

    elist1 = events_indices_repo.find_indices(
        chain_id, events_index.address, events_index.event, {"value": 2}
    )
    elist2 = events_indices_repo.find_indices(
        chain_id, events_index.address, events_index.event, {"some": 3}
    )
    elist3 = events_indices_repo.find_indices(
        chain_id, events_index.address, events_index.event, {"from": "0x12"}
    )
    assert len(elist1) == 0
    assert len(elist2) == 0
    assert len(elist3) == 0


@given(args_subset_and_superset())
def test_is_softer_filter_than(sub_and_sup: Tuple[Dict[str, Any], Dict[str, Any]]):
    sub, sup = sub_and_sup
    assert is_softer_filter_than(sub, sup)


def test_is_softer_filter_than_edge():
    assert is_softer_filter_than(None, None)
    assert is_softer_filter_than(None, {})
    assert is_softer_filter_than({}, {})
    assert is_softer_filter_than({}, None)
    assert not is_softer_filter_than({"x": 1}, None)
    assert is_softer_filter_than(None, {"x": 1})
    assert is_softer_filter_than({}, {"x": 1})


def test_events_index_to_dict():
    index_data = EventsIndexData()
    index_data.set_range(11000, 14000, True)
    chain_id = 1
    address = "0x1234"
    event = "Transfer"
    args = {"from": "0x2345"}
    index = EventsIndex(chain_id, address, event, args, index_data)
    assert index.to_dict() == {
        "chain_id": 1,
        "address": address,
        "event": event,
        "args": args,
        "data": {"start_block": 8000, "mask": "0x38"},
    }

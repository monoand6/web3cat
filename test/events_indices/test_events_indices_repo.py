from typing import Any, Dict, Tuple
from hypothesis import given
from probe.events_indices.index import EventsIndex
from probe.events_indices.index_data import EventsIndexData

from probe.events_indices.repo import EventsIndicesRepo, args_is_subset
from events_indices.strategies import args_subset_and_superset
from fixtures.events_indices import events_indices_repo


def test_read_write(events_indices_repo: EventsIndicesRepo):
    events_index_data = EventsIndexData(100)
    events_index = EventsIndex(
        1, "0x1234", "TestEvent", {"from": "0x1234", "value": 1}, events_index_data
    )
    events_indices_repo.save(events_index)
    list1 = events_indices_repo.find_indices(events_index.address, events_index.event)
    list2 = events_indices_repo.find_indices(
        events_index.address, events_index.event, events_index.args
    )
    list3 = events_indices_repo.find_indices(
        events_index.address, events_index.event, {"from": "0x1234"}
    )
    list4 = events_indices_repo.find_indices(
        events_index.address, events_index.event, {"value": 1}
    )
    list5 = events_indices_repo.find_indices(
        events_index.address, events_index.event, {}
    )

    assert len(list1) == 1
    assert len(list2) == 1
    assert len(list3) == 1
    assert len(list4) == 1
    assert len(list5) == 1
    assert list1[0] == events_index
    assert list1[0] == list2[0]
    assert list2[0] == list3[0]
    assert list3[0] == list4[0]
    assert list4[0] == list5[0]

    elist1 = events_indices_repo.find_indices(
        events_index.address, events_index.event, {"value": 2}
    )
    elist2 = events_indices_repo.find_indices(
        events_index.address, events_index.event, {"some": 3}
    )
    elist3 = events_indices_repo.find_indices(
        events_index.address, events_index.event, {"from": "0x12"}
    )
    assert len(elist1) == 0
    assert len(elist2) == 0
    assert len(elist3) == 0


@given(args_subset_and_superset())
def test_args_is_subset(sub_and_sup: Tuple[Dict[str, Any], Dict[str, Any]]):
    sub, sup = sub_and_sup
    assert args_is_subset(sub, sup)


def test_args_is_subset_edge():
    assert args_is_subset(None, None)
    assert args_is_subset(None, {})
    assert args_is_subset({}, {})
    assert args_is_subset({}, None)
    assert not args_is_subset({"x": 1}, None)
    assert args_is_subset(None, {"x": 1})
    assert args_is_subset({}, {"x": 1})

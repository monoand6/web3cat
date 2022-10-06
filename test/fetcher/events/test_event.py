from hypothesis import given
from events.strategies import event
from fetcher.events.event import Event


@given(event())
def test_event_tuples(event: Event):
    assert Event.from_tuple(event.to_tuple()) == event


@given(event())
def test_event_to_from_dict(event: Event):
    assert Event.from_dict(event.to_dict()) == event


@given(event())
def test_event_matches_self_filter(event: Event):
    assert event.matches_filter(event.args)


def test_event_matches_filter():
    e = Event(1, 1, "0x1234", 1, "0x5678", "Transfer", None)
    filter1 = None
    filter2 = {}
    filter3 = {"from": "0x5555"}
    filter4 = {"from": "0x5555", "value": 20}
    filter5 = {"from": "0x5555", "value": [1, 2, 3]}
    filter6 = {"from": "0x5555", "value": [10, 20, 30]}
    filter7 = {"from": "0x5555", "value": [[1, 2], [2, 3]]}
    assert e.matches_filter(filter1)
    assert e.matches_filter(filter2)
    assert not e.matches_filter(filter3)
    assert not e.matches_filter(filter4)
    assert not e.matches_filter(filter5)
    assert not e.matches_filter(filter6)
    assert not e.matches_filter(filter7)

    e.args = {"from": "0x5566"}
    assert e.matches_filter(filter1)
    assert e.matches_filter(filter2)
    assert not e.matches_filter(filter3)
    assert not e.matches_filter(filter4)
    assert not e.matches_filter(filter5)
    assert not e.matches_filter(filter6)
    assert not e.matches_filter(filter7)

    e.args = {"from": "0x5555"}
    assert e.matches_filter(filter1)
    assert e.matches_filter(filter2)
    assert e.matches_filter(filter3)
    assert not e.matches_filter(filter4)
    assert not e.matches_filter(filter5)
    assert not e.matches_filter(filter6)
    assert not e.matches_filter(filter7)

    e.args = {"from": "0x5555", "value": [1, 2]}
    assert e.matches_filter(filter1)
    assert e.matches_filter(filter2)
    assert e.matches_filter(filter3)
    assert not e.matches_filter(filter4)
    assert not e.matches_filter(filter5)
    assert not e.matches_filter(filter6)
    assert e.matches_filter(filter7)

    e.args = {"from": "0x5555", "value": [1, 2, 3]}
    assert e.matches_filter(filter1)
    assert e.matches_filter(filter2)
    assert e.matches_filter(filter3)
    assert not e.matches_filter(filter4)
    assert e.matches_filter(filter5)
    assert not e.matches_filter(filter6)
    assert not e.matches_filter(filter7)

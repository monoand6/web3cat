from hypothesis import given
from events.strategies import event
from fetcher.events.event import Event


@given(event())
def test_tuples(event: Event):
    assert Event.from_tuple(event.to_tuple()) == event

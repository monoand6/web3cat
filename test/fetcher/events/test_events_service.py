from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import integers
from fetcher.events.service import EventsService

EVENTS_START_BLOCK = 15630000
EVENTS_END_BLOCK = 15632000


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    start=integers(EVENTS_START_BLOCK, EVENTS_END_BLOCK),
    end=integers(EVENTS_START_BLOCK, EVENTS_END_BLOCK),
)
def test_events_service_basic_cache(
    start: int,
    end: int,
    events_service: EventsService,
    web3_event_mock,
):
    try:
        if start > end:
            start, end = end, start
        web3_event_mock.events_fetched = 0
        events = events_service.get_events(1, web3_event_mock, start, end)
        assert web3_event_mock.events_fetched == len(events)
        events2 = events_service.get_events(1, web3_event_mock, start, end)
        assert events == events2
        assert web3_event_mock.events_fetched == len(events)
    finally:
        events

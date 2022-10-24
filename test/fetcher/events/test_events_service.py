import chunk
from random import shuffle
from typing import List
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import integers, lists
from fetcher.events.service import EventsService
from fetcher.events_indices.constants import BLOCKS_PER_BIT
from web3 import Web3

EVENTS_START_BLOCK = 15627000
EVENTS_END_BLOCK = 15632000
TOTAL_EVENTS = 6759


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
@given(
    start=integers(EVENTS_START_BLOCK, EVENTS_END_BLOCK),
    end=integers(EVENTS_START_BLOCK, EVENTS_END_BLOCK),
)
def test_events_service_basic_cache_snapped_blocks(
    start: int,
    end: int,
    events_service: EventsService,
    w3_mock: Web3,
):
    try:
        if start > end:
            start, end = end, start
        # snapping to grid so that returned number of events == fetched number of events
        start = start // BLOCKS_PER_BIT * BLOCKS_PER_BIT
        end = end // BLOCKS_PER_BIT * BLOCKS_PER_BIT
        events = events_service.get_events(w3_mock, start, end)
        assert w3_mock.events_fetched == len(events)
        events2 = events_service.get_events(w3_mock, start, end)
        assert events == events2
        assert w3_mock.events_fetched == len(events)
    finally:
        events_service.clear_cache()
        w3_mock.events_fetched = 0


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
@given(
    start=integers(EVENTS_START_BLOCK, EVENTS_END_BLOCK),
    end=integers(EVENTS_START_BLOCK, EVENTS_END_BLOCK),
)
def test_events_service_basic_cache(
    start: int,
    end: int,
    events_service: EventsService,
    w3_mock: Web3,
):
    try:
        if start > end:
            start, end = end, start
        start = start // BLOCKS_PER_BIT * BLOCKS_PER_BIT
        end = end // BLOCKS_PER_BIT * BLOCKS_PER_BIT
        events = events_service.get_events(w3_mock, start, end)
        fetched = w3_mock.events_fetched
        events2 = events_service.get_events(w3_mock, start, end)
        assert events == events2
        assert w3_mock.events_fetched == fetched
    finally:
        events_service.clear_cache()
        w3_mock.events_fetched = 0


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
@given(
    chunks=integers(1, 5),
)
def test_events_service_cache_chunks(
    chunks: int,
    events_service: EventsService,
    w3_mock: Web3,
):
    try:
        chunk_size = (EVENTS_END_BLOCK - EVENTS_START_BLOCK) // chunks
        ranges = []
        for start in range(EVENTS_START_BLOCK, EVENTS_END_BLOCK, chunk_size):
            end = min(start + chunk_size, EVENTS_END_BLOCK)
            ranges.append((start, end))
        shuffle(ranges)
        for start, end in ranges:
            end = min(start + chunk_size, EVENTS_END_BLOCK)
            events = events_service.get_events(w3_mock, start, end)
            fetched = w3_mock.events_fetched
            events2 = events_service.get_events(w3_mock, start, end)
            assert events == events2
            assert w3_mock.events_fetched == fetched
        assert w3_mock.events_fetched == TOTAL_EVENTS
    finally:
        events_service.clear_cache()
        w3_mock.events_fetched = 0

import chunk
from random import shuffle
from typing import List
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import permutations
from fetcher.calls.service import CallsService
from fetcher.events_indices.constants import BLOCKS_PER_BIT

CALLS_START_BLOCK = 15632000
CALLS_END_BLOCK = 15642000


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=500)
@given(blocks=permutations(range(CALLS_START_BLOCK, CALLS_END_BLOCK, 50)))
def test_calls_service_basic_cache(
    blocks: List[int],
    calls_service: CallsService,
    web3_calls_mock,
):
    try:
        assert web3_calls_mock.number_of_calls == 0
        for b in blocks:
            calls_service.get_call(1, web3_calls_mock, b)
        assert web3_calls_mock.number_of_calls == len(blocks)
        for b in blocks:
            calls_service.get_call(1, web3_calls_mock, b)
        assert web3_calls_mock.number_of_calls == len(blocks)
    finally:
        calls_service.clear_cache()
        web3_calls_mock.number_of_calls = 0

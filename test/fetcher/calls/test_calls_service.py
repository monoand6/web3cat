import chunk
from random import shuffle
from typing import List
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import permutations
from web3cat.fetcher.calls.service import CallsService
from web3cat.fetcher.events_indices.constants import BLOCKS_PER_BIT
from web3 import Web3

CALLS_START_BLOCK = 15632000
CALLS_END_BLOCK = 15642000


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=500)
@given(blocks=permutations(range(CALLS_START_BLOCK, CALLS_END_BLOCK, 50)))
def test_calls_service_basic_cache(
    blocks: List[int], calls_service: CallsService, w3_mock: Web3
):
    try:
        assert w3_mock.number_of_calls == 0
        for b in blocks:
            calls_service.get_call(w3_mock, b)
        assert w3_mock.number_of_calls == len(blocks)
        for b in blocks:
            calls_service.get_call(w3_mock, b)
        assert w3_mock.number_of_calls == len(blocks)
    finally:
        calls_service.clear_cache()
        w3_mock.number_of_calls = 0

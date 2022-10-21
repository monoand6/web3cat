import chunk
from random import shuffle
from typing import List
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import permutations
from fetcher.balances.service import BalancesService
from fetcher.events_indices.constants import BLOCKS_PER_BIT

BALANCES_START_BLOCK = 15632000
BALANCES_END_BLOCK = 15642000


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=500)
@given(blocks=permutations(range(BALANCES_START_BLOCK, BALANCES_END_BLOCK, 50)))
def test_balances_service_basic_cache(
    blocks: List[int],
    balances_service: BalancesService,
    web3_balances_mock,
):
    address = "0x5777d92f208679db4b9778590fa3cab3ac9e2168"
    try:
        assert web3_balances_mock.number_of_balances == 0
        for b in blocks:
            balances_service.get_balance(address, b)
        assert web3_balances_mock.number_of_balances == len(blocks)
        for b in blocks:
            balances_service.get_balance(address, b)
        assert web3_balances_mock.number_of_balances == len(blocks)
    finally:
        balances_service.clear_cache()
        web3_balances_mock.number_of_balances = 0

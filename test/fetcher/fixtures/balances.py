from sqlite3 import Connection
from typing import Dict
import pytest
from fetcher.balances.service import BalancesService

from fixtures.general import Web3

from fetcher.balances.repo import BalancesRepo
from fetcher.balances.balance import Balance


@pytest.fixture
def balances_repo(conn: Connection) -> BalancesRepo:
    """
    Instance of db.BalancesRepo
    """
    return BalancesRepo(conn)


BALANCES_START_BLOCK = 15632000
BALANCES_END_BLOCK = 15642000
BALANCES = {b: b * 1000 for b in range(BALANCES_START_BLOCK, BALANCES_END_BLOCK, 100)}


class Web3BalanceMock:
    number_of_balances: int

    def __init__(self):
        self.number_of_balances = 0
        self.min_bn = min(BALANCES.keys())
        self.max_bn = max(BALANCES.keys())

    @property
    def eth(self):
        return self

    @property
    def chain_id(self):
        return 1

    def toChecksumAddress(self, addr: str):
        return Web3.toChecksumAddress(addr)

    def get_balance(self, address: str, block_identifier: int) -> str:
        self.number_of_balances += 1
        block_identifier = block_identifier // 100 * 100
        offset = self._address_offset(address)
        if block_identifier in BALANCES:
            return BALANCES[block_identifier] + offset
        if block_identifier < self.min_bn:
            return BALANCES[self.min_bn] + offset

        return BALANCES[self.max_bn] + offset

    def _address_offset(self, address: str) -> int:
        return int(address[2:], 16) % 1000


@pytest.fixture(scope="session")
def web3_balances_mock() -> Web3BalanceMock:
    """
    Instance of web3 balanceOf balance mock
    """
    return Web3BalanceMock()


@pytest.fixture
def balances_service(
    balances_repo: BalancesRepo, web3_balances_mock: Web3BalanceMock
) -> BalancesService:
    """
    Instance of balances.Balances
    """
    return BalancesService(balances_repo, web3_balances_mock)

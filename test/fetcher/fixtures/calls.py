import re
from fetcher.calls import Call
from sqlite3 import Connection
from typing import Any, Dict, List
import os
import json
import pytest
from web3.auto import w3
from web3.contract import Contract

from fetcher.calls.repo import CallsRepo
from fetcher.calls.service import CallsService
from fixtures.general import Web3Mock


@pytest.fixture
def calls_repo(conn: Connection) -> CallsRepo:
    """
    Instance of calls.CallsRepo
    """
    return CallsRepo(conn)


@pytest.fixture
def calls_service(calls_repo: CallsRepo) -> CallsService:
    """
    Instance of calls.CallsRepo
    """
    return CallsService(calls_repo)


class Web3ContractCallMock(Web3Mock):
    _calls: Dict[int, Call]
    number_of_calls: int

    def __init__(self):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        compound_address = "0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643"
        dai_abi = json.load(open(f"{current_folder}/erc20_abi.json", "r"))
        token: Contract = w3.eth.contract(address=dai_address, abi=dai_abi)
        self._call = token.functions.balanceOf(compound_address)
        calls = json.load(open(f"{current_folder}/calls.json"))
        self._calls = {e["blockNumber"]: Call.from_dict(e) for e in calls}
        self.number_of_calls = 0
        self.min_bn = min(self._calls.keys())
        self.max_bn = max(self._calls.keys())

    def call(self, block_identifier: int) -> str:
        self.number_of_calls += 1
        block_identifier = block_identifier // 10 * 10
        if block_identifier in self._calls:
            return self._calls[block_identifier].response
        if block_identifier < self.min_bn:
            return self._calls[self.min_bn].response

        return self._calls[self.max_bn].response

    @property
    def abi(self):
        return self._call.abi

    @property
    def args(self):
        return self._call.args

    @property
    def address(self):
        return self._call.address


@pytest.fixture(scope="session")
def web3_calls_mock() -> Web3ContractCallMock:
    """
    Instance of web3 balanceOf call mock
    """
    return Web3ContractCallMock()

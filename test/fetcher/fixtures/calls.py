from fetcher.calls import Call
from sqlite3 import Connection
from typing import Any, Dict, List
import os
import json
import pytest
from web3.auto import w3
from web3.contract import Contract

from fetcher.calls.repo import CallsRepo


@pytest.fixture
def calls_repo(conn: Connection) -> CallsRepo:
    """
    Instance of calls.CallsRepo
    """
    return CallsRepo(conn)


class Web3ContractCallMock:
    _calls: List[Call]

    def __init__(self):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        compound_address = "0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643"
        dai_abi = json.load(open("{current_folder}/erc20_abi.json", "r"))
        token: Contract = w3.eth.contract(address=dai_address, abi=dai_abi)
        self._call = token.functions.balanceOf(compound_address)
        calls = json.load(open(f"{current_folder}/calls.json"))
        self._calls = [Call.from_dict(e) for e in calls]


@pytest.fixture(scope="session")
def web3_calls_mock() -> Web3ContractCallMock:
    """
    Instance of web3 balanceOf call mock
    """
    return Web3ContractCallMock()

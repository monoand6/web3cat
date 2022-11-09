import re
from web3cat.fetcher.calls import Call
from sqlite3 import Connection
from typing import Any, Dict, List
import os
import json
import pytest
from web3.auto import w3
from web3.contract import Contract

from web3cat.fetcher.calls.repo import CallsRepo
from web3cat.fetcher.calls.service import CallsService
from fixtures.general import Web3Mock
from web3 import Web3


@pytest.fixture
def calls_repo(cache_path: str, w3_mock: Web3) -> CallsRepo:
    """
    Instance of calls.CallsRepo
    """
    return CallsRepo(cache_path=cache_path, w3=w3_mock)


@pytest.fixture
def calls_service(calls_repo: CallsRepo, w3_mock: Web3) -> CallsService:
    """
    Instance of calls.CallsRepo
    """
    return CallsService(calls_repo, w3=w3_mock)

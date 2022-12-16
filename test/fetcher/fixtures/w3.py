import json
import os
from random import randint
from typing import Any, Dict, List
import pytest

from web3 import Web3
from web3.contract import Contract
from web3.auto import w3 as w3auto
from web3cat.fetcher.calls.call import Call

from web3cat.fetcher.events.event import Event
from web3.exceptions import BlockNotFound
from eth_typing.encoding import HexStr

LATEST_BLOCK = 15839990
INTIAL_TIME = 1438200000


class Web3EventFilterMock:
    _events: List[Dict[str, Any]]

    def __init__(self, events: List[Dict[str, Any]]):
        self._events = events

    def get_all_entries(self) -> List[Dict[str, Any]]:
        return self._events


class Web3Mock:
    _events: List[Event]
    _balances: List[int]

    number_of_calls: int
    events_fetched: int
    event_name: str
    number_of_balances: int

    def __init__(self):
        current_folder = os.path.realpath(os.path.dirname(__file__))
        events = json.load(open(f"{current_folder}/events.json"))
        self._events = [Event.from_dict(e) for e in events]
        self.address = "0x6b175474e89094c44da98b954eedeac495271d0f"
        self.events_fetched = 0
        self.event_name = "Transfer"

        self.number_of_calls = 0

        self._balances = {b: b * 1000 for b in range(15632000, 15642000, 100)}
        self.number_of_balances = 0
        self.min_bn = min(self._balances.keys())
        self.max_bn = max(self._balances.keys())

        dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        compound_address = "0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643"
        dai_abi = json.load(open(f"{current_folder}/erc20_abi.json", "r"))
        token: Contract = w3auto.eth.contract(address=dai_address, abi=dai_abi)
        self._call = token.functions.balanceOf(compound_address)
        calls = json.load(open(f"{current_folder}/calls.json"))
        self._calls = {e["blockNumber"]: Call.from_dict(e) for e in calls}
        self.number_of_calls = 0
        self.min_bn_call = min(self._calls.keys())
        self.max_bn_call = max(self._calls.keys())

    def call(self, block_identifier: int) -> str:
        self.number_of_calls += 1
        block_identifier = block_identifier // 10 * 10
        if block_identifier in self._calls:
            return self._calls[block_identifier].response
        if block_identifier < self.min_bn_call:
            return self._calls[self.min_bn_call].response

        return self._calls[self.max_bn_call].response

    def create_filter(
        self, fromBlock: int, toBlock: int, argument_filters: Dict[str, Any] | None
    ) -> Web3EventFilterMock:
        events = [
            e
            for e in self._events
            if e.block_number >= fromBlock
            and e.block_number <= toBlock
            and e.matches_filter(argument_filters)
        ]
        self.events_fetched += len(events)
        return Web3EventFilterMock([e.to_dict() for e in events])

    def get_block(self, num: int | str) -> Dict[str, Any]:
        self.number_of_calls += 1
        if num == "latest":
            return self.get_raw_block(LATEST_BLOCK)
        if num > LATEST_BLOCK:
            raise BlockNotFound()
        if num < 1:
            raise ValueError("Should never query block 0")
        return self.get_raw_block(num)

    def get_raw_block(self, number: int) -> Dict[str, Any]:
        return {
            "hash": HexStr(Web3.keccak(number)).hex(),
            "number": number,
            "timestamp": 1438200000 + number * 13,
        }

    def get_balance(self, address: str, block_identifier: int) -> str:
        self.number_of_balances += 1
        block_identifier = block_identifier // 100 * 100
        offset = self._address_offset(address)
        if block_identifier in self._balances:
            return self._balances[block_identifier] + offset
        if block_identifier < self.min_bn:
            return self._balances[self.min_bn] + offset

        return self._balances[self.max_bn] + offset

    def _address_offset(self, address: str) -> int:
        return int(address[2:], 16) % 1000

    def to_checksum_address(self, addr: str) -> str:
        return Web3.to_checksum_address(addr)

    @property
    def eth(self):
        return self

    @property
    def chain_id(self):
        return 1

    @property
    def abi(self):
        return self._call.abi

    @property
    def args(self):
        return self._call.args


@pytest.fixture(scope="session")
def w3_mock() -> Web3:
    """
    Mock instance of Web3
    """
    return Web3Mock()

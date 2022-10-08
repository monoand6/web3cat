from __future__ import annotations
import sys
import json
from typing import Any, Dict, List, Tuple
from fetcher.db import connection_from_path
from fetcher.calls.call import Call
from fetcher.calls.repo import CallsRepo
from web3 import Web3
from web3.contract import ContractFunction
from web3.auto import w3 as w3auto

from fetcher.utils import calldata, json_response, short_address


class CallsService:
    """
    Service for web3 calls.

    The sole purpose of this service is to cache web 3 calls and
    serve them on subsequent calls.

    The exact flow goes like this
    ::

                +---------------+                 +-------+ +-----------+
                | CallsService  |                 | Web3  | | CallsRepo |
                +---------------+                 +-------+ +-----------+
        ---------------\ |                             |           |
        | Request call |-|                             |           |
        |--------------| |                             |           |
                         |                             |           |
                         | Find response               |           |
                         |---------------------------------------->|
                         |                             |           |
                         | If not found: call web3     |           |
                         |---------------------------->|           |
                         |                             |           |
                         | Save response               |           |
                         |---------------------------------------->|
            -----------\ |                             |           |
            | Response |-|                             |           |
            |----------| |                             |           |
                         |                             |           |


    Args:
        calls_repo: Repo of calls
    """

    _calls_repo: CallsRepo

    def __init__(self, calls_repo: CallsRepo):
        self._calls_repo = calls_repo
        self._last_progress_bar_length = 0

    @staticmethod
    def create(cache_path: str = "cache.sqlite3") -> CallsService:
        """
        Create an instance of :class:`CallsService`

        Args:
            cache_path: path for the cache database

        Returns:
            An instance of :class:`CallsService`
        """
        conn = connection_from_path(cache_path)
        calls_repo = CallsRepo(conn)
        return CallsService(calls_repo)

    def get_call(
        self,
        call: ContractFunction,
        block_number: int,
    ) -> Call:
        """
        Get call specified by parameters.

        Args:
            call: class:`web3.contract.ContractCall` specifying contract and call arguments.
            block_number: fetch call for this block

        Returns:
            A fetched calls
        """
        chain_id = call.web3.eth.chain_id
        data = calldata(call)
        calls = self._calls_repo.find(
            chain_id, call.address, data, block_number, block_number + 1
        )
        if len(calls) > 0:
            return calls[0]

        resp = json.loads(json_response(call.call(block_identifier=block_number)))
        call_item = Call(chain_id, call.address, data, block_number, resp)
        self._calls_repo.save([call_item])
        self._calls_repo.commit()

        calls = self._calls_repo.find(
            chain_id, call.address, data, block_number, block_number + 1
        )
        return calls[0]

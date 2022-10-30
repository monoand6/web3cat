from __future__ import annotations
from itertools import product
import json
from typing import List
from web3.contract import ContractFunction
from fetcher.calls.call import Call

from fetcher.core import Core
from fetcher.utils import calldata, json_response, print_progress
from fetcher.calls.repo import CallsRepo


class CallsService(Core):
    """
    Service for making contract static calls.

    The sole purpose of this service is to cache web 3 calls and
    serve them on subsequent calls.

    **Request/Response flow**

    ::

                +---------------+                 +-------+ +-----------+
                | CallsService  |                 | Web3  | | CallsRepo |
                +---------------+                 +-------+ +-----------+
        ---------------  |                             |           |
        | Request call |-|                             |           |
        |--------------| |                             |           |
                         |                             |           |
                         | Find response               |           |
                         |---------------------------------------->|
                         |                             |           |
                         | If not found: call Web3     |           |
                         |---------------------------->|           |
                         |                             |           |
                         | Save response               |           |
                         |---------------------------------------->|
            -----------  |                             |           |
            | Response |-|                             |           |
            |----------| |                             |           |
                         |                             |           |


    Args:
        calls_repo: :class:`CallsRepo` instance
        kwargs: Args for the :class:`fetcher.core.Core`
    """

    _calls_repo: CallsRepo

    def __init__(self, calls_repo: CallsRepo, **kwargs):
        super().__init__(**kwargs)
        self._calls_repo = calls_repo

    @staticmethod
    def create(**kwargs) -> CallsService:
        """
        Create an instance of :class:`CallsService`

        Args:
            kwargs: Args for the :class:`fetcher.core.Core`

        Returns:
            An instance of :class:`CallsService`
        """
        calls_repo = CallsRepo(**kwargs)
        return CallsService(calls_repo, **kwargs)

    def get_call(
        self,
        call: ContractFunction,
        block_number: int,
    ) -> Call:
        """
        Make contract call specified by parameters.

        Args:
            call: :class:`web3.contract.ContractFunction` specifying contract, function
                  and call arguments.
            block_number: get call at this block

        Returns:
            A fetched :class:`fetcher.calls.Call`
        """

        data = calldata(call)
        calls = list(
            self._calls_repo.find(call.address, data, block_number, block_number + 1)
        )
        if len(calls) > 0:
            return calls[0]

        resp = json.loads(json_response(call.call(block_identifier=block_number)))
        call_item = Call(self.chain_id, call.address, data, block_number, resp)
        self._calls_repo.save([call_item])
        self._calls_repo.conn.commit()

        return call_item

    def get_calls(
        self,
        calls: List[ContractFunction],
        block_numbers: List[int],
    ) -> Call:
        """
        Make a list of contract calls specified by parameters.

        Note:
            Each call is make for each block

        Args:
            calls: a list of :class:`web3.contract.ContractFunction` specifying contract, function
                  and call arguments.
            block_number: A list of blocks

        Returns:
            A fetched :class:`fetcher.calls.Call`
        """
        ids = set(
            f"{call.address.lower()}|{calldata(call)}|{block_number}"
            for call, block_number in product(calls, block_numbers)
        )
        calls_idx = {}
        for call in calls:
            calls_idx[f"{call.address.lower()}|{calldata(call)}"] = call
        idx = {}
        cached_calls = list(
            self._calls_repo.find_many(
                [(call.address, calldata(call)) for call in calls], block_numbers
            )
        )
        for call in cached_calls:
            call_id = f"{call.address.lower()}|{call.calldata}|{call.block_number}"
            idx[call_id] = call
            ids.remove(call_id)

        for i, call_id in enumerate(ids):
            print_progress(i, len(ids), f"Fetching {len(ids)} calls")
            address, cd, block_number = call_id.split("|")
            call = calls_idx[f"{address}|{cd}"]
            fetched_call = self.get_call(call, int(block_number))
            idx[call_id] = fetched_call
        if len(ids) > 0:
            print_progress(len(ids), len(ids), f"Fetching {len(ids)} calls")
        out = []

        for call in calls:
            for block_number in block_numbers:
                idx_id = f"{call.address.lower()}|{calldata(call)}|{block_number}"
                out.append(idx[idx_id])

        return out

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._calls_repo.purge()
        self._calls_repo.conn.commit()

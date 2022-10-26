from __future__ import annotations
import json
from web3.contract import ContractFunction
from fetcher.calls.call import Call

from fetcher.core import Core
from fetcher.utils import calldata, json_response
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

        calls = list(
            self._calls_repo.find(call.address, data, block_number, block_number + 1)
        )
        return calls[0]

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._calls_repo.purge()
        self._calls_repo.conn.commit()

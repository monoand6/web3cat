from __future__ import annotations
from sqlite3 import Timestamp
import sys
import json
from typing import Any, Dict, List, Tuple
from fetcher.core import Core
from fetcher.balances.balance import Balance
from fetcher.balances.repo import BalancesRepo
from web3 import Web3
from web3.contract import ContractFunction
from web3.auto import w3 as w3auto

from fetcher.utils import json_response, print_progress, short_address


class BalancesService(Core):
    """
    Service for getting and caching Web3 ETH balances.

    Since there's no easy way of getting ETH balance deltas from
    Web3, this service literally queries and caches ETH balance
    for every desired date and address. Hence it's not as effective as
    querying token balances, based on fetching Transfer events (deltas)
    in batches.

    **Request - Response flow**

    ::

                +-----------------+                +-------+ +---------------+
                | BalancesService |                | Web3  | | BalancesRepo  |
                +-----------------+                +-------+ +---------------+
        ---------------  |                             |             |
        | Request call |-|                             |             |
        |--------------| |                             |             |
                         |                             |             |
                         | Find Balance                |             |
                         |------------------------------------------>|
                         |                             |             |
                         | If not found: call Web3     |             |
                         |---------------------------->|             |
                         |                             |             |
                         | Save response               |             |
                         |------------------------------------------>|
            -----------  |                             |             |
            | Response |-|                             |             |
            |----------| |                             |             |
                         |                             |             |

    Args:
        balances_repo: An instance of :class:`BalancesRepo`
    """

    _balances_repo: BalancesRepo

    def __init__(self, balances_repo: BalancesRepo, **kwargs):
        super().__init__(**kwargs)
        self._balances_repo = balances_repo

    @staticmethod
    def create(**kwargs) -> BalancesService:
        """
        Create an instance of :class:`BalancesService`

        Args:
            cache_path: path for the cache database
            rpc: Ethereum rpc url. If ``None``, `Web3 auto detection <https://web3py.savethedocs.io/en/stable/providers.html#how-automated-detection-works>`_ is used

        Returns:
            An instance of :class:`BalancesService`
        """
        balances_repo = BalancesRepo(**kwargs)
        return BalancesService(balances_repo, **kwargs)

    def get_balances(self, addresses: List[str], blocks: List[int]) -> List[Balance]:
        """
        Get ETH balances for a list of blocks and addresses.

        Args:
            addresses: a list of addresses for ETH balances
            blocks: a list of blocks for ETH balances

        Returns:
            A list of :class:`Balance` for addresses and blocks. The size of a list = ``len(addresses) * len(blocks)``
        """
        addresses = [addr.lower() for addr in addresses]
        total_number = len(addresses) * len(blocks)
        if total_number == 0:
            return []
        out = []
        for addr in addresses:
            for i, b in enumerate(blocks):
                print_progress(
                    i,
                    len(blocks),
                    f"Fetching eth balances for {short_address(addr)}",
                )
                out.append(self.get_balance(addr, b))
            print_progress(
                len(blocks),
                len(blocks),
                f"Fetching eth balances for {short_address(addr)}",
            )
        return out

    def get_balance(self, address: str, block_number: int) -> Balance:
        """
        Get ETH balance for a block and an address.

        Args:
            address: The address for the ETH balance
            block: The block for which the ETH balance is fetched

        Returns:
            ETH balance
        """
        address = address.lower()
        balances = list(
            self._balances_repo.find(address, block_number, block_number + 1)
        )
        if len(balances) > 0:
            return balances[0]

        resp = json.loads(
            json_response(
                self.w3.eth.get_balance(
                    self.w3.toChecksumAddress(address), block_identifier=block_number
                )
            )
        )
        balance_item = Balance(self.chain_id, block_number, address, resp / 10**18)
        self._balances_repo.save([balance_item])
        self._balances_repo.conn.commit()

        balances = list(
            self._balances_repo.find(address, block_number, block_number + 1)
        )
        return balances[0]

    def clear_cache(self):
        """
        Delete all cached ETH balances
        """
        self._balances_repo.purge()
        self._balances_repo.conn.commit()

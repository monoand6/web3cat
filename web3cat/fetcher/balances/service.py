from __future__ import annotations
import json
from typing import List
from web3cat.fetcher.core import Core
from web3cat.fetcher.balances.balance import Balance
from web3cat.fetcher.balances.repo import BalancesRepo
from web3cat.fetcher.utils import json_response, print_progress


class BalancesService(Core):
    """
    Service for getting and caching ETH balances.

    Since there's no easy way of getting ETH balance deltas from
    Web3, this service queries and caches ETH balance
    for every desired date and address.

    Note:
        It's not as effective as querying token balances, based on fetching Transfer events (deltas)
        in batches. Use it with caution.

    **Request/Response flow**

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
        kwargs: Args for the :class:`fetcher.core.Core` class
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
            kwargs: Args for the :class:`fetcher.core.Core` class

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
            A list of :class:`Balance` for addresses and blocks.
            The size of a list = ``len(addresses) * len(blocks)``
        """
        addresses = {[addr.lower() for addr in addresses]}
        blocks = set(blocks)
        total_number = len(addresses) * len(blocks)
        if total_number == 0:
            return []

        cached_balances = list(
            self._balances_repo.find(addresses, min(blocks), max(blocks) + 1)
        )
        cached_balances_idx = {}
        for bal in cached_balances:
            key = f"{bal.address}|{bal.block_number}"
            cached_balances_idx[key] = bal

        cached_num = 0
        for addr in addresses:
            for block in blocks:
                key = f"{addr}|{block}"
                if key in cached_balances_idx:
                    cached_num += 1

        num_fetch = len(addresses) * len(blocks) - cached_num
        out = []
        i = 0
        for addr in addresses:
            for block in blocks:
                key = f"{addr}|{block}"
                if key in cached_balances_idx:
                    out.append(cached_balances_idx[key])
                    continue
                print_progress(
                    i,
                    num_fetch,
                    f"Fetching {num_fetch} eth balances",
                )
                out.append(self._fetch_balance_and_save(addr, block))
                i += 1

        if num_fetch > 0:
            print_progress(
                num_fetch,
                num_fetch,
                f"Fetching {num_fetch} eth balances",
            )
        return out

    def get_balance(self, address: str, block_number: int) -> Balance:
        """
        Get ETH balance for a block and an address.

        Args:
            address: The address for the ETH balance
            block_number: The block number for which the ETH balance is fetched

        Returns:
            ETH balance
        """
        address = address.lower()
        balances = list(
            self._balances_repo.find([address], block_number, block_number + 1)
        )
        if len(balances) > 0:
            return balances[0]

        return self._fetch_balance_and_save(address, block_number)

    def clear_cache(self):
        """
        Delete all cached ETH balances
        """
        self._balances_repo.purge()
        self._balances_repo.conn.commit()

    def _fetch_balance_and_save(self, address: str, block_number: int) -> Balance:
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
        return balance_item

from __future__ import annotations
from sqlite3 import Timestamp
import sys
import json
from typing import Any, Dict, List, Tuple
from fetcher.db import connection_from_path
from fetcher.balances.balance import Balance
from fetcher.balances.repo import BalancesRepo
from web3 import Web3
from web3.contract import ContractFunction
from web3.auto import w3 as w3auto

from fetcher.utils import get_chain_id, json_response, print_progress, short_address


class BalancesService:
    """
    Service for web3 balances.

    The sole purpose of this service is to cache web 3 balances and
    serve them on subsequent balances.



    Args:
        balances_repo: Repo of balances
    """

    _w3: Web3
    _balances_repo: BalancesRepo
    _chain_id: int | None

    def __init__(self, balances_repo: BalancesRepo, w3: Web3):
        self._balances_repo = balances_repo
        self._w3 = w3
        self._chain_id = None

    @staticmethod
    def create(
        cache_path: str = "cache.sqlite3", rpc: str | None = None
    ) -> BalancesService:
        """
        Create an instance of :class:`BalancesService`

        Args:
            cache_path: path for the cache database

        Returns:
            An instance of :class:`BalancesService`
        """
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))

        conn = connection_from_path(cache_path)
        balances_repo = BalancesRepo(conn)
        return BalancesService(balances_repo, w3)

    @property
    def chain_id(self) -> int:
        """
        Ethereum chain_id
        """
        if self._chain_id is None:
            self._chain_id = get_chain_id(self._w3)
        return self._chain_id

    def get_balances(self, addresses: List[str], blocks: List[int]) -> List[Balance]:
        total_number = len(addresses) * len(blocks)
        if total_number == 0:
            return []
        out = []
        for addr in addresses:
            for i, b in enumerate(blocks):
                print_progress(
                    f"Balances.get_balances_{addr}",
                    i,
                    len(blocks),
                    f"Fetching eth balances for {short_address(addr)}",
                )
                out.append(self.get_balance(addr, b))
            print_progress(
                f"Balances.get_balances_{addr}",
                len(blocks),
                len(blocks),
                f"Fetching eth balances for {short_address(addr)}",
            )
        return out

    def get_balance(self, address: str, block_number: int) -> Balance:
        balances = list(
            self._balances_repo.find(
                self.chain_id, address, block_number, block_number + 1
            )
        )
        if len(balances) > 0:
            return balances[0]

        resp = json.loads(
            json_response(
                self._w3.eth.get_balance(
                    self._w3.toChecksumAddress(address), block_identifier=block_number
                )
            )
        )
        balance_item = Balance(self.chain_id, block_number, address, resp / 10**18)
        self._balances_repo.save([balance_item])
        self._balances_repo.commit()

        balances = list(
            self._balances_repo.find(
                self.chain_id, address, block_number, block_number + 1
            )
        )
        return balances[0]

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._balances_repo.purge()
        self._balances_repo.commit()

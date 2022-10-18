from __future__ import annotations
import sys
import json
from typing import Any, Dict, List, Tuple
from fetcher.db import connection_from_path
from fetcher.balances.balance import Balance
from fetcher.balances.repo import BalancesRepo
from web3 import Web3
from web3.contract import ContractFunction
from web3.auto import w3 as w3auto

from fetcher.utils import json_response


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

    def __init__(self, balances_repo: BalancesRepo, w3: Web3):
        self._balances_repo = balances_repo
        self._w3 = w3

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

    def get_balance(self, chain_id: int, address: str, block_number: int) -> Balance:
        balances = list(
            self._balances_repo.find(chain_id, address, block_number, block_number + 1)
        )
        if len(balances) > 0:
            return balances[0]

        resp = json.loads(
            json_response(
                self._w3.eth.get_balance(address, block_identifier=block_number)
            )
        )
        balance_item = Balance(chain_id, block_number, address, resp)
        self._balances_repo.save([balance_item])
        self._balances_repo.commit()

        balances = list(
            self._balances_repo.find(chain_id, address, block_number, block_number + 1)
        )
        return balances[0]

    def clear_cache(self):
        """
        Delete all cached entries
        """
        self._balances_repo.purge()
        self._balances_repo.commit()

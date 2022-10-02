from __future__ import annotations
import json
import os
from typing import Any, Dict
from web3 import Web3
from web3.auto import w3 as w3auto
from web3.contract import Contract
from fetcher.db import connection_from_path

from fetcher.erc20_metas.erc20_meta import ERC20Meta
from fetcher.erc20_metas.repo import ERC20MetasRepo


class ERC20MetaService:
    _w3: Web3
    _cache: Dict[str, Any]
    _chain_id: int
    _erc20_abi: Dict[str, Any]
    _erc20_metas_repo: ERC20MetasRepo

    def __init__(self, erc20_metas_repo: ERC20MetasRepo, w3: Web3):
        self._w3 = w3
        self._chain_id = w3.eth.chain_id
        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(f"{current_folder}/tokens.json", "r") as f:
            self._cache = json.load(f)
        with open(f"{current_folder}/erc20_abi.json", "r") as f:
            self._erc20_abi = json.load(f)
        self._erc20_metas_repo = erc20_metas_repo

    def create(
        cache_path: str = "cache.sqlite3", rpc: str | None = None
    ) -> ERC20MetaService:
        conn = connection_from_path(cache_path)
        erc20_metas_repo = ERC20MetasRepo(conn)
        w3 = w3auto
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
        return ERC20MetaService(erc20_metas_repo, w3)

    def get(self, token: str) -> ERC20Meta | None:
        cached_token = self._get_from_cache(token)
        if cached_token:
            return cached_token
        cached_token = self._erc20_metas_repo.find(self._chain_id, token)
        if cached_token:
            return cached_token
        if not token.startswith("0x"):
            raise ValueError(f"Could not find token `{token}`")
        contract: Contract = self._w3.eth.contract(
            address=self._w3.toChecksumAddress(token), abi=self._erc20_abi
        )
        decimals = contract.functions.decimals().call()
        name = contract.functions.name().call()
        symbol = contract.functions.symbol().call()
        meta = ERC20Meta(self._chain_id, token.lower(), name, symbol, decimals)
        self._erc20_metas_repo.save([meta])
        self._erc20_metas_repo.commit()
        return meta

    def _get_from_cache(self, token: str) -> ERC20Meta | None:
        token = token.lower()
        chain_id = str(self._chain_id)
        if not chain_id in self._cache:
            return None
        if not token in self._cache[chain_id]:
            return None
        data = self._cache[chain_id][token]
        return ERC20Meta(
            self._chain_id,
            data["address"],
            data["name"],
            data["symbol"],
            data["decimals"],
        )

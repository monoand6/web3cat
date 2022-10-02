from typing import List
from fetcher.events.event import Event
from fetcher.db import Repo
from fetcher.erc20_metas.erc20_meta import ERC20Meta


class ERC20MetasRepo(Repo):
    def find(self, chain_id: int, token: str) -> Event | None:
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM erc20_metas WHERE chain_id = ? AND (symbol = ? OR address = ?)",
            (chain_id, token.lower(), token.lower()),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return ERC20Meta.from_tuple(row)

    def save(self, erc20_metas: List[ERC20Meta]):
        cursor = self._connection.cursor()
        rows = [e.to_tuple() for e in erc20_metas]
        cursor.executemany(
            "INSERT INTO erc20_metas VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

from typing import List
from web3cat.fetcher.core import Core
from web3cat.fetcher.erc20_metas.erc20_meta import ERC20Meta


class ERC20MetasRepo(Core):
    """
    Reading and writing :class:`ERC20Meta` to database.
    """

    def find(self, token: str) -> ERC20Meta | None:
        """
        Find a :class:`ERC20Meta`.

        Args:
            token: token symbol or address

        Returns:
            An instance of :class:`ERC20Meta` or ``None`` if not found

        Examples:
            ::

                repo.find("DAI")
                repo.find("0x6B175474E89094C44Da98b954EedeAC495271d0F")
                # => same results
        """
        token = token.lower()
        cursor = self.conn.execute(
            "SELECT * FROM erc20_metas WHERE chain_id = ? AND (symbol = ? OR address = ?)",
            (self.chain_id, token, token),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return ERC20Meta.from_row(row)

    def save(self, erc20_metas: List[ERC20Meta]):
        """
        Save :class:`ERC20Meta` to database.

        Args:
            erc20_metas: a list of :class:`ERC20Meta` to save
        """
        rows = [e.to_row() for e in erc20_metas]
        self.conn.executemany(
            "INSERT INTO erc20_metas VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING", rows
        )

    def purge(self):
        """
        Clear all database entries
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM erc20_metas")

from __future__ import annotations
import json
from typing import Any, Dict, Tuple
from web3.contract import Contract


class ERC20Meta:
    """
    ERC20 token metadata (name, symbol, decimals).

    Note:
        The convention is to use ``address``, ``name`` and ``symbol`` in lowercase format.
        This is not in line with EIP55 but makes things more uniform and
        simpler.
    """

    #: Ethereum chain_id
    chain_id: int
    #: Token address (lowercase)
    address: str
    #: Token name (lowercase)
    name: str
    #: Token symbol (lowercase)
    symbol: str
    #: Token decimals
    decimals: int
    #: :class:`web3.Contract` for the token
    contract: Contract

    def __init__(
        self,
        chain_id: int,
        address: str,
        name: str,
        symbol: str,
        decimals: int,
        contract: Contract,
    ):
        self.chain_id = chain_id
        self.address = address.lower()
        self.name = name.lower()
        self.symbol = symbol.lower()
        self.decimals = decimals
        self.contract = contract

    @staticmethod
    def from_row(row: Tuple[int, str, str, str, int]) -> ERC20Meta:
        """
        Deserialize from web3cat.database row

        Args:
            row: database row
        """
        return ERC20Meta(contract=None, *row)

    def to_row(self) -> Tuple[int, str, str, str, int]:
        """
        Serialize to database row

        Returns:
            database row
        """

        return (self.chain_id, self.address, self.name, self.symbol, self.decimals)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert :class:`ERC20Meta` to dict
        """
        return {
            "chainId": self.chain_id,
            "address": self.address,
            "name": self.name,
            "symbol": self.symbol,
            "decimals": self.decimals,
        }

    @staticmethod
    def from_dict(dct: Dict[str, Any]):
        """
        Create :class:`Call` from dict
        """

        return ERC20Meta(
            chain_id=dct["chainId"],
            address=dct["address"],
            name=dct["name"],
            symbol=dct["symbol"],
            decimals=dct["decimals"],
            contract=None,
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f"ERC20Meta({json.dumps(self.to_dict())})"

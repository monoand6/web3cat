from __future__ import annotations
from typing import Tuple


class ERC20Meta:
    chain_id: int
    _address: str
    name: str
    _symbol: str
    decimals: int

    def __init__(
        self, chain_id: int, address: str, name: str, symbol: str, decimals: int
    ):
        self.chain_id = chain_id
        self.address = address
        self.name = name
        self.symbol = symbol
        self.decimals = decimals

    def from_tuple(tuple: Tuple[int, str, str, str, int]) -> ERC20Meta:
        return ERC20Meta(*tuple)

    def to_tuple(self) -> Tuple[int, str, str, str, int]:
        return (self.chain_id, self._address, self.name, self._symbol, self.decimals)

    @property
    def address(self) -> str:
        return self._address

    @address.setter
    def address(self, val: str):
        self._address = val.lower()

    @property
    def symbol(self) -> str:
        return self._symbol

    @symbol.setter
    def symbol(self, val: str):
        self._symbol = val.lower()

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f'ERC20Meta({{"chain_id": {self.chain_id}, "address": {self.address}, "name": {self.name}, "symbol": {self.symbol}, "decimals": {self.decimals}}})'

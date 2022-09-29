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

    def __repr__(self):
        return f'ERC20Meta({{"chain_id": {self.chain_id}, "address": {self.address}, "name": {self.name}, "symbol": {self.symbol}, "decimals": {self.decimals}}})'

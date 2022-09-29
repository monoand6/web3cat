class ERC20Meta:
    chain_id: int
    _address: str
    name: str
    symbol: str
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

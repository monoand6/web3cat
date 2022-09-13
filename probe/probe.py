from web3 import Web3


class Probe:
    path: str
    rpc: str

    def __init__(self, path: str, rpc: str):
        self.path = path
        self.rpc = rpc

from db import BlocksDB
from web3 import Web3


class Blocks:
    _blocksDB: BlocksDB
    _w3: Web3

    def __init__(self, blocks_db: BlocksDB, w3: Web3):
        self._blocks_db = blocks_db
        self._w3 = w3

    def get_block_for_timestamp():
        pass

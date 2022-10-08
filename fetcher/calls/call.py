from __future__ import annotations
import json
from typing import Any, Dict, Tuple


class Call:
    """
    Call represents a call to Ethereum function with a response
    """

    #: Ethereum chain_id
    chain_id: int
    #: Number of the block for this call
    block_number: int
    _address: str
    _calldata: str
    #: Response received for the call
    response: Dict[str, Any]

    def __init__(
        self,
        chain_id: int,
        block_number: int,
        address: str,
        calldata: str,
        response: Dict[str, Any],
    ):
        self.chain_id = chain_id
        self.block_number = block_number
        self.address = address
        self.calldata = calldata
        self.response = response

    @property
    def address(self):
        """
        Contract address for this call
        """
        return self._address

    @address.setter
    def address(self, val: str):
        self._address = val.lower()

    @property
    def calldata(self):
        """
        Calldata for this call
        """
        return self._calldata

    @calldata.setter
    def calldata(self, val: str):
        self._calldata = val.lower()

    @staticmethod
    def from_tuple(tuple: Tuple[int, int, str, str, str]) -> Call:
        """
        Deserialize from database row

        Args:
            tuple: database row
        """
        call = Call(*tuple)
        call.response = json.loads(call.response)
        return call

    def to_tuple(self) -> Tuple[int, int, str, str, str]:
        """
        Serialize to database row

        Returns:
            database row
        """
        return (
            self.chain_id,
            self.block_number,
            self.address,
            self.calldata,
            json.dumps(self.response),
        )

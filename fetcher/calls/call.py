from __future__ import annotations
import json
from typing import Any, Dict, Tuple


class Call:
    """
    Call represents a call to Ethereum function with a response
    """

    #: Ethereum chain_id
    chain_id: int
    _address: str
    _calldata: str
    #: Number of the block for this call
    block_number: int
    #: Response received for the call
    response: Dict[str, Any]

    def __init__(
        self,
        chain_id: int,
        address: str,
        calldata: str,
        block_number: int,
        response: Dict[str, Any],
    ):
        self.chain_id = chain_id
        self.address = address
        self.calldata = calldata
        self.block_number = block_number
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
    def from_tuple(tuple: Tuple[int, str, str, int, str]) -> Call:
        """
        Deserialize from database row

        Args:
            tuple: database row
        """
        call = Call(*tuple)
        call.response = json.loads(call.response)
        return call

    def to_tuple(self) -> Tuple[int, str, str, int, str]:
        """
        Serialize to database row

        Returns:
            database row
        """
        return (
            self.chain_id,
            self.address,
            self.calldata,
            self.block_number,
            json.dumps(self.response),
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert :class:`Call` to dict
        """
        return {
            "chainId": self.chain_id,
            "address": self.address,
            "calldata": self.calldata,
            "blockNumber": self.block_number,
            "response": self.response,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        """
        Create :class:`Call` from dict
        """

        return Call(
            chain_id=d["chainId"],
            address=d["address"],
            calldata=d["calldata"],
            block_number=d["blockNumber"],
            response=d["response"],
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f'Call({{"chain_id":{self.chain_id}, "address": {self.address}, "calldata": {self.calldata}, "block_number": {self.block_number}, "response": {json.dumps(self.response)}}})'

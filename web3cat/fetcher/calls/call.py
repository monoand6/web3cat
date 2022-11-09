from __future__ import annotations
import json
from typing import Any, Dict, Tuple


class Call:
    """
    Call represents a static call to Ethereum contract function
    """

    #: Ethereum chain_id
    chain_id: int
    #: Contract address for this call
    address: str
    #: Calldata for this call
    calldata: str
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
        self.address = address.lower()
        self.calldata = calldata.lower()
        self.block_number = block_number
        self.response = response

    @staticmethod
    def from_row(row: Tuple[int, str, str, int, str]) -> Call:
        """
        Deserialize from web3cat.database row

        Args:
            row: database row
        """
        call = Call(*row)
        call.response = json.loads(call.response)
        return call

    def to_row(self) -> Tuple[int, str, str, int, str]:
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
    def from_dict(dct: Dict[str, Any]):
        """
        Create :class:`Call` from dict
        """

        return Call(
            chain_id=dct["chainId"],
            address=dct["address"],
            calldata=dct["calldata"],
            block_number=dct["blockNumber"],
            response=dct["response"],
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return f"Call({json.dumps(self.to_dict())})"

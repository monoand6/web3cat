"""
Different utils for working with Web3.
"""

from typing import Any, Dict, Union
from hexbytes import HexBytes
from eth_typing.encoding import HexStr
from web3.datastructures import AttributeDict
import json


class Web3JsonEncoder(json.JSONEncoder):
    """
    Custom encoder to parse `Web3 <https://web3py.readthedocs.io/en/stable/>`_ responses.
    By default `Web3 <https://web3py.readthedocs.io/en/stable/>`_ returns
    responses as :code:`AttributeDict` with binary values.
    :class:`Web3JsonEncoder` thansforms it into the :code:`0x...`
    hex format.
    """

    def default(self, obj: Any) -> Union[Dict[Any, Any], HexStr]:
        """
        Convert Web3 response to :code:`dict`
        """
        if isinstance(obj, AttributeDict):
            return {k: v for k, v in obj.items()}
        if isinstance(obj, HexBytes):
            return HexStr(obj.hex())
        if isinstance(obj, (bytes, bytearray)):
            return HexStr(HexBytes(obj).hex())
        return json.JSONEncoder.default(self, obj)


def json_response(response: AttributeDict) -> str:
    """
    Convert :code:`AttributeDict` to standard json string

    Args:
        response: a `Web3 <https://web3py.readthedocs.io/en/stable/>`_ response

    Returns:
        json string
    """
    return json.dumps(response, cls=Web3JsonEncoder)


def short_address(address: str) -> str:
    """
    Converts ethereum address to short version (for display purposes only).

    Args:
        address: Ethereum address to shorten

    Returns:
        Short version of the address.

    Examples:
        ::

            print(short_address("0x6B175474E89094C44Da98b954EedeAC495271d0F"))
            # 0x6B17...1d0F

    """
    return f"{address[:6]}...{address[37:]}"

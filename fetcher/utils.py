"""
Utility functions.
"""

import sys
from typing import Any, Dict, Union
from hexbytes import HexBytes
from eth_typing.encoding import HexStr
from web3.datastructures import AttributeDict
from web3.contract import ContractFunction, get_abi_input_types
from web3.auto import w3
from eth_utils import function_abi_to_4byte_selector

import json


class Web3JsonEncoder(json.JSONEncoder):
    """
    Custom encoder to parse `Web3 <https://web3py.readthedocs.io/en/stable/>`_ responses.
    By default `Web3 <https://web3py.readthedocs.io/en/stable/>`_ returns
    responses as ``AttributeDict`` with binary values.
    :class:`Web3JsonEncoder` thansforms it into the :code:`0x...`
    hex format.
    """

    def default(self, obj: Any) -> Union[Dict[Any, Any], HexStr]:
        """
        Convert Web3 response to ``dict``
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
    Convert ``AttributeDict`` to standard json string

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


def calldata(call: ContractFunction) -> str:
    """
    Hex calldata for :class:`web3.contract.ContractFunction` call

    Args:
        func: A web3 call

    Returns:
        Hex data (starting with 0x, lowercase)
    """

    selector = HexBytes(function_abi_to_4byte_selector(call.abi)).hex()
    abi_types = get_abi_input_types(call.abi)
    bytes_calldata = w3.codec.encode(abi_types, call.args)
    return selector + HexBytes(bytes_calldata).hex()[2:]


last_progress_bar_length = 0


def print_progress(
    iteration: int,
    total: int,
    prefix: str = "",
    suffix: str = "",
    decimals: int = 1,
    bar_length: int = 20,
) -> int:
    """
    Call in a loop to create terminal progress bar.

    Args:
        iteration: current iteration
        total: total iterations
        prefix: prefix string
        suffix: suffix string
        decimals: positive number of decimals in percent complete
        bar_length: character length of bar
    """
    global last_progress_bar_length

    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = "█" * filled_length + "-" * (bar_length - filled_length)
    text = "%s |%s| %s%s %s\r" % (prefix, bar, percents, "%", suffix)
    sys.stdout.write("%s\r" % (" " * last_progress_bar_length)),
    sys.stdout.write(text),
    last_progress_bar_length = len(text)

    if iteration == total:
        sys.stdout.write("\n")
        sys.stdout.flush()

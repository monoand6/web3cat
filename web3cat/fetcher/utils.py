"""
Utility functions.
"""

import sys
from typing import Any, Dict, Union
import json
from hexbytes import HexBytes
from eth_typing.encoding import HexStr
from web3.datastructures import AttributeDict
from web3.contract import ContractFunction, get_abi_input_types
from web3.auto import w3
from eth_utils import function_abi_to_4byte_selector


class Web3JsonEncoder(json.JSONEncoder):
    """
    Custom encoder to parse `Web3 <https://web3py.readthedocs.io/en/stable/>`_ responses.
    By default `Web3 <https://web3py.readthedocs.io/en/stable/>`_ returns
    responses as ``AttributeDict`` with binary values.
    :class:`Web3JsonEncoder` thansforms it into the :code:`0x...`
    hex format.
    """

    def default(self, o: Any) -> Union[Dict[Any, Any], HexStr]:
        """
        Convert Web3 response to ``dict``
        """
        if isinstance(o, AttributeDict):
            return {k: v for k, v in o.items()}
        if isinstance(o, HexBytes):
            return HexStr(o.hex())
        if isinstance(o, (bytes, bytearray)):
            return HexStr(HexBytes(o).hex())
        return json.JSONEncoder.default(self, o)


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
    return f"{address[:6]}...{address[38:]}"


def calldata(call: ContractFunction) -> str:
    """
    Hex calldata for :class:`web3.contract.ContractFunction` call

    Args:
        call: A web3 call

    Returns:
        Hex data (starting with 0x, lowercase)
    """

    selector = HexBytes(function_abi_to_4byte_selector(call.abi)).hex()
    abi_types = get_abi_input_types(call.abi)
    bytes_calldata = w3.codec.encode(abi_types, call.args)
    return selector + HexBytes(bytes_calldata).hex()[2:]


LAST_PROGRESS_BAR_LENGTH = 0
LAST_PERCENT = 0


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
    global LAST_PROGRESS_BAR_LENGTH
    global LAST_PERCENT

    # Avoid too much output
    if iteration != total and iteration > 0 and iteration / total - LAST_PERCENT < 0.01:
        return

    LAST_PERCENT = iteration / total
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    vert_bar = "█" * filled_length + "-" * (bar_length - filled_length)
    text = f"{prefix} |{vert_bar}| {percents}% {suffix}\r"
    _ = sys.stdout.write(f"{' ' * LAST_PROGRESS_BAR_LENGTH}\r")
    _ = sys.stdout.write(text)
    LAST_PROGRESS_BAR_LENGTH = len(text)

    if iteration == total:
        sys.stdout.write("\n")
        sys.stdout.flush()
        LAST_PERCENT = 0
        LAST_PROGRESS_BAR_LENGTH = 0

from typing import Any, Dict, Union
from hexbytes import HexBytes
from eth_typing.encoding import HexStr
from web3.datastructures import AttributeDict
import json


class Web3JsonEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Union[Dict[Any, Any], HexStr]:
        if isinstance(obj, AttributeDict):
            return {k: v for k, v in obj.items()}
        if isinstance(obj, HexBytes):
            return HexStr(obj.hex())
        if isinstance(obj, (bytes, bytearray)):
            return HexStr(HexBytes(obj).hex())
        return json.JSONEncoder.default(self, obj)


def json_response(response: AttributeDict) -> str:
    return json.dumps(response, cls=Web3JsonEncoder)

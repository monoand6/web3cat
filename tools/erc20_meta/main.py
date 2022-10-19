from typing import Any, Dict, List
import requests
import json
import os

urls = [
    "https://static.optimism.io/optimism.tokenlist.json",
    "https://tokens.uniswap.org/",
    "https://raw.githubusercontent.com/compound-finance/token-list/master/compound.tokenlist.json",
    "https://tokens.coingecko.com/uniswap/all.json",
    "https://www.gemini.com/uniswap/manifest.json",
    "https://raw.githubusercontent.com/SetProtocol/uniswap-tokenlist/main/set.tokenlist.json",
    "https://app.tryroll.com/tokens.json",
    "https://extendedtokens.uniswap.org/",
    "https://celo-org.github.io/celo-token-list/celo.tokenlist.json",
    "https://raw.githubusercontent.com/The-Blockchain-Association/sec-notice-list/master/ba-sec-list.json",
    "https://bridge.arbitrum.io/token-list-42161.json",
    "https://static.optimism.io/optimism.tokenlist.json",
]


def get_data(url: str) -> List[Dict[str, Any]]:
    resp = requests.get(url=url)
    return resp.json()["tokens"]


def fetch_tokens():
    data = {}
    for url in urls:
        tokens = get_data(url)
        for t in tokens:
            chain_id = t["chainId"]
            if not chain_id in data:
                data[chain_id] = {}
            data[chain_id][t["symbol"].lower()] = t
            data[chain_id][t["address"].lower()] = t
    return data


current_folder = os.path.realpath(os.path.dirname(__file__))


data = fetch_tokens()
sorted_data = {}
for chain_id in sorted(data.keys()):
    for idx in sorted(data[chain_id].keys()):
        if not chain_id in sorted_data:
            sorted_data[chain_id] = {}
        sorted_data[chain_id][idx] = data[chain_id][idx]
with open(f"{current_folder}/tokens.json", "w") as f:
    json.dump(sorted_data, f)

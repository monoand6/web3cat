import os
import json

current_folder = os.path.realpath(os.path.dirname(__file__))

chain_ids_map = {
    1: "eth",
    42161: "arb",
    43114: "avax",
    56: "bsc",
    250: "ftm",
    100: "gnosis",
    1666600000: "harmony",
    128: "heco",
    1284: "moonbeam",
    1285: "moonriver",
    10: "op",
    137: "polygon",
}

out = {}

for chain_id, chain_name in chain_ids_map.items():
    with open(f"{current_folder}/inputs/{chain_name}_chainlink.csv", "r") as f:
        out[chain_id] = {}
        for row in f.readlines():
            entries = row.split(",")
            if len(entries) < 6:
                continue
            quote = entries[0]
            name = entries[1]
            kind = entries[2]
            address = entries[3]
            if kind.lower() != "crypto":
                continue
            if not quote.lower().endswith('usd"'):
                continue

            ticker = quote.split("/")[0][:-1].lower()
            out[chain_id][ticker] = {"address": address.lower()}

with open(f"{current_folder}/outputs/result.json", "w") as f:
    json.dump(out, f)

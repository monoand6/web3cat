from __future__ import annotations
from functools import cached_property
from datetime import datetime
from typing import Dict, Any, List
import os
import json
import polars as pl
from web3.contract import Contract
from web3.constants import ADDRESS_ZERO

from web3cat.data.core import DataCore

POOLS = [
    {
        "token": "dai",
        "facade": "0xf6f4F24ae50206A07B8B32629AeB6cb1837d854F",
        "manager": "0x672461Bfc20DD783444a830Ad4c38b345aB6E2f7",
    },
    {
        "token": "usdc",
        "facade": "0x61fbb350e39cc7bF22C01A469cf03085774184aa",
        "manager": "0x95357303f995e184A7998dA6C6eA35cC728A1900",
    },
    {
        "token": "weth",
        "facade": "0xC59135f449bb623501145443c70A30eE648Fa304",
        "manager": "0x5887ad4Cb2352E7F01527035fAa3AE0Ef2cE2b9B",
    },
    {
        "token": "wbtc",
        "facade": "0xAfae62D1b38d635a3089A36B27f3c1Acc4fa3243",
        "manager": "0xc62BF8a7889AdF1c5Dc4665486c7683ae6E74e0F",
    },
    {
        "token": "wsteth",
        "facade": "0x04d692bCB03D4b410CBB5Bf09967eB3ce8D12546",
        "manager": "0xe0bCE4460795281d39c91da9B0275BcA968293de",
    },
]


class GearboxData(DataCore):
    """
    Data for the gearbox protocol

    Args:
        start: Starting timepoint
        end: Ending timepoint
    """

    def __init__(self, start: int | datetime, end: int | datetime, **kwargs):
        super().__init__(start, end, **kwargs)
        if self._blocks_service.chain_id != 1:
            raise ValueError(
                "For gearbox Mainnet RPC is required (chain_id = 1), got chain_id = {self.blocks_service.chain_id}"
            )

    def health_factor(
        self,
        borrower_address: str,
        token: str,
        timepoints: List[int | datetime],
    ) -> pl.DataFrame:
        """
        Health factor for borrower in a credit pool specified by token.

        Args:
            borrower_address: address of the borrower
            token: token that determines the credit pool
            timepoints: a list of timepoints

        Returns:
            A dataframe with fields

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp for the snapshot of the balance                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :class:`int`               | Number of the block                                                          |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``transaction_hash`` | :class:`str`               | Hash of the liquidation tx                                                   |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        token_meta = self._erc20_metas_service.get(token)
        pool = self._get_pool(token_meta.symbol)
        manager = self._credit_manager(pool["manager"])
        facade = self._credit_facade(pool["facade"])
        blocks = self._resolve_timepoints(timepoints)
        account_addresses = self._calls_service.get_calls(
            [
                manager.functions.creditAccounts(
                    self.w3.toChecksumAddress(borrower_address)
                )
            ],
            [b.number for b in blocks],
        )
        account_addresses = [acc.response for acc in account_addresses]

        out = []
        for acc, b in zip(account_addresses, blocks):
            if acc == ADDRESS_ZERO:
                out.append(
                    {
                        "timestamp": b.timestamp,
                        "date": datetime.fromtimestamp(b.timestamp),
                        "block_number": b.number,
                        "health_factor": 0,
                    }
                )
                continue
            resp = self._calls_service.get_call(
                facade.functions.calcCreditAccountHealthFactor(
                    self.w3.toChecksumAddress(acc)
                ),
                b.number,
            )
            out.append(
                {
                    "timestamp": b.timestamp,
                    "date": datetime.fromtimestamp(b.timestamp),
                    "block_number": b.number,
                    "health_factor": resp.response / 10**4,
                }
            )
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "health_factor": pl.Float64,
            },
        ).sort("block_number")

    @cached_property
    def liquidations(self) -> pl.DataFrame:
        """
        Dataframe with liquidation events.

        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | Field                | Type                       | Description                                                                  |
        +======================+============================+==============================================================================+
        | ``timestamp``        | :attr:`numpy.int64`        | Timestamp for the snapshot of the balance                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``date``             | :class:`datetime.datetime` | Date for the timestamp                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``block_number``     | :class:`int`               | Number of the block                                                          |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``transaction_hash`` | :class:`str`               | Hash of the liquidation tx                                                   |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``token``            | :class:`str`               | Name of the token for the Gearbox pool                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``borrower``         | :class:`str`               | Address of the liquidated borrower                                           |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``liquidator``       | :class:`str`               | Address of the liquidator                                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``to``               | :class:`str`               | Address of the liquidation funds receiver                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``remaining_funds``  | :attr:`numpy.float64`      | Amount of remaining funds                                                    |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        events = []
        for pool in POOLS:
            token = self._erc20_metas_service.get(pool["token"])
            facade = self._credit_facade(pool["facade"])
            pool_events = self._events_service.get_events(
                facade.events.LiquidateCreditAccount,
                self.from_block_number,
                self.to_block_number,
            )
            for e in pool_events:
                e.args["remainingFunds"] /= 10**token.decimals
                e.args["token"] = token.symbol
            events += pool_events

        blocks = self._resolve_timepoints([e.block_number for e in events])
        out = []
        for b, e in zip(blocks, events):
            out.append(
                {
                    "timestamp": b.timestamp,
                    "date": datetime.fromtimestamp(b.timestamp),
                    "block_number": b.number,
                    "transaction_hash": e.transaction_hash,
                    "token": e.args["token"],
                    "borrower": e.args["borrower"],
                    "liquidator": e.args["liquidator"],
                    "to": e.args["to"],
                    "remaining_funds": e.args["remainingFunds"],
                }
            )
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "transaction_hash": pl.Utf8,
                "token": pl.Utf8,
                "borrower": pl.Utf8,
                "liquidator": pl.Utf8,
                "to": pl.Utf8,
                "remaining_funds": pl.Float64,
            },
        ).sort("block_number")

    def _get_pool(self, token: str) -> Dict[str, Any] | None:
        for pool in POOLS:
            if pool["token"].lower() == token.lower():
                return pool
        return None

    def _credit_account(self, address: str) -> Contract:
        return self.w3.eth.contract(
            address=self.w3.toChecksumAddress(address),
            abi=self._credit_account_abi,
        )

    def _credit_facade(self, address: str) -> Contract:
        return self.w3.eth.contract(
            address=self.w3.toChecksumAddress(address),
            abi=self._credit_facade_abi,
        )

    def _credit_manager(self, address: str) -> Contract:
        return self.w3.eth.contract(
            address=self.w3.toChecksumAddress(address),
            abi=self._credit_manager_abi,
        )

    @cached_property
    def _credit_account_abi(self) -> Dict[str, Any] | None:
        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(
            f"{current_folder}/abi/credit_account_abi.json", "r", encoding="utf-8"
        ) as f:
            return json.load(f)

    @cached_property
    def _credit_facade_abi(self) -> Dict[str, Any] | None:
        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(
            f"{current_folder}/abi/credit_facade_abi.json", "r", encoding="utf-8"
        ) as f:
            return json.load(f)

    @cached_property
    def _credit_manager_abi(self) -> Dict[str, Any] | None:
        current_folder = os.path.realpath(os.path.dirname(__file__))
        with open(
            f"{current_folder}/abi/credit_manager_abi.json", "r", encoding="utf-8"
        ) as f:
            return json.load(f)

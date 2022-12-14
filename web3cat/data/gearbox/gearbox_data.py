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

DATA_COMPRESSOR = "0x0a2CA503153Cd5CB2892a0928ac0F71F49a3c194"
ACCOUNT_FACTORY = "0x444cd42baeddeb707eed823f7177b9abcc779c04"
ACCOUNT_FACTORY_DEPLOY_BLOCK = 13810899


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

    def credit_account_data(
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
        | ``block_number``     | :attr:`numpy.int64`        | Number of the block                                                          |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``tvl``              | :attr:`numpy.float64`      | Position Total value                                                         |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``health_factor``    | :attr:`numpy.float64`      | Position Health factor                                                       |
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
                        "tvl": 0,
                        "health_factor": 0,
                    }
                )
                continue
            resps = self._calls_service.get_calls(
                [
                    facade.functions.calcCreditAccountHealthFactor(
                        self.w3.toChecksumAddress(acc)
                    ),
                    facade.functions.calcTotalValue(self.w3.toChecksumAddress(acc)),
                ],
                [b.number],
            )
            out.append(
                {
                    "timestamp": b.timestamp,
                    "date": datetime.fromtimestamp(b.timestamp),
                    "block_number": b.number,
                    "tvl": resps[1].response[0] / 10**token_meta.decimals,
                    "health_factor": resps[0].response / 10**4,
                }
            )
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "tvl": pl.Float64,
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
        for pool in self.pools:
            token = pool["token"]
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

    def credit_accounts(self, timepoint: int | datetime) -> List[str]:
        """
        A list of all credit accounts for a specific timepoint

        Args:
            timepoint: a timepoint to fetch credit accounts for
        """
        block = self._resolve_timepoints([timepoint])[0]
        tail = self._calls_service.get_call(
            self._account_factory.functions.tail(), block.number
        ).response.lower()
        head = self._calls_service.get_call(
            self._account_factory.functions.head(), block.number
        ).response.lower()
        out = [head]
        while True:
            acc = self._calls_service.get_call(
                self._account_factory.functions.getNext(
                    self.w3.toChecksumAddress(out[-1])
                ),
                block.number,
            ).response.lower()
            out.append(acc)
            if acc == tail:
                break
        return out

    def _get_pool(self, token: str) -> Dict[str, Any] | None:
        for pool in self.pools:
            if pool["token"].symbol.lower() == token.lower():
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
    def pools(self) -> List[Dict[str, Any]]:
        """
        Gearbox pools data.
        """
        credit_manager_data = self._calls_service.get_call(
            self._data_compressor.functions.getCreditManagersList(),
            self.to_block_number,
        )
        out = []
        for entry in credit_manager_data.response:
            if entry[14] == ADDRESS_ZERO:
                # skip v1
                continue
            out.append(
                {
                    "token": self._erc20_metas_service.get(entry[1]),
                    "manager": entry[0],
                    "facade": entry[14],
                    "configurator": entry[15],
                    "pool": entry[2],
                    "borrow_rate": entry[5] / 10**27,
                    "fee_interest": entry[21] / 10**4,
                    "fee_liquidation": entry[22] / 10**4,
                    "liquidation_discount": entry[23] / 10**4,
                    "fee_liquidation_expired": entry[24] / 10**4,
                    "liquidation_discount_expired": entry[25] / 10**4,
                }
            )
        return out

    @cached_property
    def _account_factory(self) -> Contract:
        current_folder = os.path.realpath(os.path.dirname(__file__))
        abi = None
        with open(
            f"{current_folder}/abi/account_factory_abi.json", "r", encoding="utf-8"
        ) as f:
            abi = json.load(f)
        return self.w3.eth.contract(
            address=self.w3.toChecksumAddress(ACCOUNT_FACTORY),
            abi=abi,
        )

    @cached_property
    def _data_compressor(self) -> Contract:
        current_folder = os.path.realpath(os.path.dirname(__file__))
        abi = None
        with open(
            f"{current_folder}/abi/data_compressor_abi.json", "r", encoding="utf-8"
        ) as f:
            abi = json.load(f)
        return self.w3.eth.contract(
            address=self.w3.toChecksumAddress(DATA_COMPRESSOR),
            abi=abi,
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

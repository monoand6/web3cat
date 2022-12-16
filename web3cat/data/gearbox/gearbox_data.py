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
        credit_accounts: List[str],
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
        | ``token``            | :class:`str`               | Pool token name                                                              |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``facade``           | :class:`str`               | Address of the credit facade                                                 |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``manager``          | :class:`str`               | Address of the credit manager                                                |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``tvl``              | :attr:`numpy.float64`      | Position Total value                                                         |
        +----------------------+----------------------------+------------------------------------------------------------------------------+
        | ``health_factor``    | :attr:`numpy.float64`      | Position Health factor                                                       |
        +----------------------+----------------------------+------------------------------------------------------------------------------+

        """
        blocks = self._resolve_timepoints(timepoints)
        blocks_idx = {b.number: b for b in blocks}
        cm_calls = [
            self._credit_account(acc).functions.creditManager()
            for acc in credit_accounts
        ]

        cm_resps = self._calls_service.get_calls(
            cm_calls,
            [self.to_block_number],
        )
        pools = [self._get_pool_by_credit_manager(cm.response) for cm in cm_resps]
        credit_accounts_v1 = set()
        for pool, ca in zip(pools, credit_accounts):
            if pool is None:
                credit_accounts_v1.add(ca)
        credit_accounts = [ca for ca in credit_accounts if not ca in credit_accounts_v1]
        pools = [p for p in pools if not p is None]
        pools_facade_idx = {
            pool["facade"].lower(): {"pool": pool, "credit_account": credit_accounts[i]}
            for i, pool in enumerate(pools)
        }

        out = []
        hf_calls = [
            self._credit_facade(pool["facade"]).functions.calcCreditAccountHealthFactor(
                self.w3.toChecksumAddress(acc)
            )
            for pool, acc in zip(pools, credit_accounts)
        ]
        tvl_calls = [
            self._credit_facade(pool["facade"]).functions.calcTotalValue(
                self.w3.toChecksumAddress(acc)
            )
            for pool, acc in zip(pools, credit_accounts)
        ]

        hf_resps = self._calls_service.get_calls(
            hf_calls,
            [b.number for b in blocks],
        )
        tvl_resps = self._calls_service.get_calls(
            tvl_calls,
            [b.number for b in blocks],
        )

        out = []
        for hf, tvl in zip(hf_resps, tvl_resps):
            block = blocks_idx[hf.block_number]
            pool = pools_facade_idx[hf.address.lower()]["pool"]
            credit_account = pools_facade_idx[hf.address.lower()]["credit_account"]
            out.append(
                {
                    "timestamp": block.timestamp,
                    "date": datetime.fromtimestamp(block.timestamp),
                    "block_number": block.number,
                    "credit_account": credit_account,
                    "token": pool["token"].symbol.lower(),
                    "facade": pools_facade_idx[hf.address.lower()]["pool"]["facade"],
                    "manager": pools_facade_idx[hf.address.lower()]["pool"]["manager"],
                    "tvl": tvl.response[0] / 10 ** pool["token"].decimals,
                    "health_factor": hf.response / 10**4,
                }
            )
        return pl.DataFrame(
            out,
            {
                "timestamp": pl.UInt64,
                "date": pl.Datetime,
                "block_number": pl.UInt64,
                "credit_account": pl.Utf8,
                "token": pl.Utf8,
                "facade": pl.Utf8,
                "manager": pl.Utf8,
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

    def credit_accounts(self, timepoint: int | datetime = None) -> List[str]:
        """
        A list of all credit accounts for a specific timepoint (incl v1)

        Args:
            timepoint: a timepoint to fetch credit accounts for
        """
        if timepoint is None:
            timepoint = self.to_block_number
        to_block = self._resolve_timepoints([timepoint])[0]
        from_block = self._blocks_service.get_blocks([ACCOUNT_FACTORY_DEPLOY_BLOCK])[0]
        initializations = self._events_service.get_events(
            self._account_factory.events.InitializeCreditAccount,
            from_block.number,
            to_block.number,
        )
        returns = self._events_service.get_events(
            self._account_factory.events.ReturnCreditAccount,
            from_block.number,
            to_block.number,
        )

        events = [
            {"kind": "mint", "block": e.block_number, "address": e.args["account"]}
            for e in initializations
        ]
        events += [
            {"kind": "burn", "block": e.block_number, "address": e.args["account"]}
            for e in returns
        ]

        events = sorted(events, key=lambda x: x["block"])
        out = set()
        for e in events:
            if e["kind"] == "mint":
                out.add(e["address"].lower())
            else:
                out.remove(e["address"].lower())
        return sorted(list(out))

    def _get_pool_by_credit_manager(self, credit_manager: str) -> Dict[str, Any] | None:
        for pool in self.pools:
            if pool["manager"].lower() == credit_manager.lower():
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

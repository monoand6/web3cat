from dataclasses import dataclass
from typing import List
from functools import cached_property
from datetime import datetime
from bokeh.plotting import Figure
from bokeh.models import GlyphRenderer
import numpy as np


from fetcher.utils import short_address
from fetcher.erc20_metas import ERC20Meta
from data.erc20s import ERC20Data
from view.wireframes.abstract import TimeseriesWireframe


@dataclass(frozen=True)
class TotalSupplyWireframe(TimeseriesWireframe):
    """
    ERC20 total supply wireframe
    """

    token: ERC20Meta

    @cached_property
    def data_key(self) -> str:
        return f"erc20_{self.token.symbol.upper()}"

    @property
    def y_axis(self) -> str:
        return f"Total Supply ({self.token.symbol.upper()})"

    def build_data(self, default_data: ERC20Data | None, **core_args) -> ERC20Data:
        data = ERC20Data(
            token=self.token.address,
            address_filter=[],
            start=self.start,
            end=self.end,
            **core_args,
        )
        if default_data is None:
            return data
        return ERC20Data(
            token=self.token.address,
            address_filter=default_data.address_filter,
            start=min(data.from_block_number, default_data.from_block_number),
            end=max(data.to_block_number, default_data.to_block_number),
        )

    def y(self, data: ERC20Data) -> List[np.float64]:
        return data.total_supply(self.x(data))["total_supply"].to_list()

    def plot(
        self, fig: Figure, x: List[datetime], y: List[np.float64], **kwargs
    ) -> GlyphRenderer:
        return fig.line(
            x,
            y,
            line_width=2,
            legend_label=self.y_axis,
            **kwargs,
        )


@dataclass(frozen=True)
class BalanceWireframe(TimeseriesWireframe):
    """
    ERC20 balances wireframe
    """

    token: ERC20Meta
    address: str

    @cached_property
    def data_key(self) -> str:
        return self.token.symbol.upper()

    @property
    def y_axis(self) -> str:
        return f"Balance ({self.token.symbol.upper()})"

    def build_data(self, default_data: ERC20Data | None, **core_args) -> ERC20Data:
        data = ERC20Data(
            token=self.token.address,
            address_filter=[self.address],
            start=self.start,
            end=self.end,
            **core_args,
        )
        if default_data is None:
            return data
        return ERC20Data(
            token=self.token.address,
            address_filter=default_data.address_filter + [self.address],
            start=min(data.from_block_number, default_data.from_block_number),
            end=max(data.to_block_number, default_data.to_block_number),
        )

    def y(self, data: ERC20Data) -> List[np.float64]:
        return data.balances([self.address], self.x(data))["balance"].to_list()

    def plot(
        self, fig: Figure, x: List[datetime], y: List[np.float64], **kwargs
    ) -> GlyphRenderer:
        return fig.line(
            x,
            y,
            line_width=2,
            legend_label=f"{short_address(self.address.lower())} balance"
            f" ({self.token.symbol.upper()})",
            **kwargs,
        )

from web3cat.dataclasses import dataclass
from typing import List
from functools import cached_property
from datetime import datetime
from bokeh.plotting import Figure
from bokeh.models import GlyphRenderer
import numpy as np


from web3cat.fetcher.utils import short_address
from web3cat.data.ethers import EtherData
from web3cat.view.wireframes.abstract import TimeseriesWireframe


@dataclass(frozen=True)
class EthBalanceWireframe(TimeseriesWireframe):
    """
    Ether balances wireframe
    """

    address: str

    @cached_property
    def data_key(self) -> str:
        return "eth_balance"

    @property
    def y_axis(self) -> str:
        return "Balance (ETH)"

    def build_data(self, default_data: EtherData | None, **core_args) -> EtherData:
        if not default_data is None:
            return default_data
        return EtherData(
            start=self.start,
            end=self.end,
            **core_args,
        )

    def y(self, data: EtherData) -> List[np.float64]:
        return data.balances([self.address], self.x(data))["balance"].to_list()

    def plot(
        self, fig: Figure, x: List[datetime], y: List[np.float64], **kwargs
    ) -> GlyphRenderer:
        return fig.line(
            x,
            y,
            line_width=2,
            legend_label=f"{short_address(self.address.lower())} balance (ETH)",
            **kwargs,
        )

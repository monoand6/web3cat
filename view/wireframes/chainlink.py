from dataclasses import dataclass
from typing import List
from functools import cached_property
from datetime import datetime
from bokeh.plotting import Figure
from bokeh.models import GlyphRenderer
import numpy as np


from fetcher.erc20_metas import ERC20Meta
from data.chainlink import ChainlinkData
from view.wireframes.abstract import TimeseriesWireframe


@dataclass(frozen=True)
class ChainlinkPricesWireframe(TimeseriesWireframe):
    """
    Chailink prices wireframe
    """

    token0: ERC20Meta
    token1: ERC20Meta

    @cached_property
    def data_key(self) -> str:
        return "chainlink_prices"

    @property
    def y_axis(self) -> str:
        return f"{self.token0.symbol.upper()} / {self.token1.symbol.upper()} price"

    def build_data(
        self, default_data: ChainlinkData | None, **core_args
    ) -> ChainlinkData:
        if default_data is None:
            return ChainlinkData(
                [self.token0.symbol, self.token1.symbol],
                self.start,
                self.end,
                **core_args,
            )
        default_data.add_token(self.token0.address)
        default_data.add_token(self.token1.address)
        return default_data

    def y(self, data: ChainlinkData) -> List[np.float64]:
        return data.prices(self.x(data), self.token0.address, self.token1.address)[
            "price"
        ].to_list()

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

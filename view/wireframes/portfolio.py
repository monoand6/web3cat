from dataclasses import dataclass
from typing import List, Dict
from functools import cached_property
from datetime import datetime
from bokeh.plotting import Figure
from bokeh.models import GlyphRenderer
from bokeh.palettes import Category20
import numpy as np
from fetcher.erc20_metas import ERC20Meta
from fetcher.utils import short_address


from data.portfolios import PortfolioData
from view.wireframes.abstract import TimeseriesWireframe


@dataclass(frozen=True)
class PortfolioWireframe(TimeseriesWireframe):
    """
    Portfolio wireframe
    """

    tokens: List[ERC20Meta]
    base_token: ERC20Meta
    addresses: List[str]

    @cached_property
    def data_key(self) -> str:
        return "portfolio"

    @property
    def y_axis(self) -> str:
        return f"Balance ({self.base_token.symbol.upper()})"

    def build_data(
        self, default_data: PortfolioData | None, **core_args
    ) -> PortfolioData:
        if not default_data is None:
            raise ValueError("Only one porfolio view per figure is allowed")
        return PortfolioData(
            addresses=self.addresses,
            tokens=[token.symbol.upper() for token in self.tokens],
            base_tokens=[self.base_token.symbol.upper()],
            start=self.start,
            end=self.end,
            numpoints=self.numpoints,
            **core_args,
        )

    def y(self, data: PortfolioData) -> List[List[np.float64]]:
        raise ValueError("Unimplemented")

    def plot(
        self, fig: Figure, x: List[datetime], y: List[np.float64], **kwargs
    ) -> GlyphRenderer:
        raise ValueError("Unimplemented")


class PortfolioByAddressWireframe(PortfolioWireframe):
    def y(self, data: PortfolioData) -> Dict[str, List[np.float64]]:
        series_dict = data.breakdown_by_address(
            self.base_token.symbol.upper()
        ).to_dict()
        exclude = ["timestamp", "date", "block_number"]
        return {k: v.to_list() for k, v in series_dict.items() if not k in exclude}

    def plot(
        self, fig: Figure, x: List[datetime], y: Dict[str, List[np.float64]], **kwargs
    ) -> GlyphRenderer:
        y = {**y}
        y.pop("total")

        colors = Category20[20] * (1 + len(y.keys()) // len(Category20[20]))
        colors = colors[: len(y.keys())]
        stackers = [*y.keys()]
        y["date"] = x
        args = {
            "stackers": stackers,
            "x": "date",
            "legend_label": [short_address(s) for s in stackers],
            "source": y,
            "alpha": 0.8,
            **kwargs,
            "color": colors,
        }
        return fig.varea_stack(**args)


class PortfolioByTokenWireframe(PortfolioWireframe):
    def y(self, data: PortfolioData) -> Dict[str, List[np.float64]]:
        series_dict = data.breakdown_by_token(self.base_token.symbol.upper()).to_dict()
        exclude = ["timestamp", "date", "block_number"]
        return {k: v.to_list() for k, v in series_dict.items() if not k in exclude}

    def plot(
        self, fig: Figure, x: List[datetime], y: Dict[str, List[np.float64]], **kwargs
    ) -> GlyphRenderer:
        y = {**y}
        y.pop("total")

        colors = Category20[20] * (1 + len(y.keys()) // len(Category20[20]))
        colors = colors[: len(y.keys())]
        y["date"] = x
        stackers = [
            f"{meta.symbol.upper()} ({self.base_token.symbol.upper()})"
            for meta in self.tokens
        ]

        args = {
            "stackers": stackers,
            "x": "date",
            "legend_label": stackers,
            "source": y,
            "alpha": 0.8,
            **kwargs,
            "color": colors,
        }
        return fig.varea_stack(**args)

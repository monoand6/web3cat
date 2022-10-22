from __future__ import annotations
from datetime import datetime, time
from enum import Enum
from typing import Any, Dict, List, Set
from bokeh.plotting import Figure, figure, show
from bokeh.models import (
    Range1d,
    LinearAxis,
    DatetimeAxis,
    NumeralTickFormatter,
    BasicTickFormatter,
)
from bokeh.palettes import Category10, Pastel1, Category20
from data.chainlink.chainlink_data import ChainlinkData
from data.erc20s.erc20_data import ERC20Data
from data.portfolios.portfolio import PortfolioData
from fetcher.blocks.service import DEFAULT_BLOCK_TIMESTAMP_GRID
from fetcher.utils import short_address
import numpy as np


class XAxis(Enum):
    TIMESERIES = 1
    ADDRESS = 2

    def bokeh_type(self):
        if self == XAxis.TIMESERIES:
            return "datetime"
        return "auto"


class YAxis:
    kind: str
    type: str | None

    def __init__(self, kind: str, type: str | None = None):
        self.kind = kind
        self.type = type

    def __repr__(self) -> str:
        return f"YAxis({self.kind},{self.type})"

    def __eq__(self, other: YAxis) -> bool:
        if not isinstance(other, YAxis):
            return False
        return self.__dict__ == other.__dict__


class DataView:
    _figure: Figure | None
    _x_axis: XAxis | None
    _y_axes: List[YAxis]
    _fig_args: Dict[str, Any]
    _numplots: int
    _colors: List[Any]

    def __init__(self, **kwargs):
        self._fig_args = kwargs
        self._figure = None
        self._x_axis = None
        self._y_axes = []
        self._colors = Category10[10]
        self._numplots = 0

    @property
    def figure(self) -> Figure:
        return self._figure

    def with_erc20_balances(
        self,
        address: str,
        token: str | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        erc20_data: ERC20Data | None = None,
        num_points: int = 100,
        **kwargs,
    ) -> DataView:
        if erc20_data is None and (start is None or end is None or token is None):
            raise ValueError(
                "erc20_data or all: start, end, and token should be specified"
            )
        if start is not None:
            erc20_data = ERC20Data.create(token, start, end)
        start = erc20_data.transfers["timestamp"][0]
        end = erc20_data.transfers["timestamp"][-1]
        step = (end - start) // num_points
        timestamps = [
            datetime.fromtimestamp(ts) for ts in range(start, end + step, step)
        ]

        balances = erc20_data.balances(address, timestamps)["balance"].to_list()
        x_axis = XAxis.TIMESERIES
        y_axis = YAxis("Balance", erc20_data.meta.symbol.upper())
        self._update_axes(x_axis, y_axis, min(balances), max(balances))
        self._figure.line(
            timestamps,
            balances,
            color=self._get_color(),
            line_width=2,
            y_range_name=str(y_axis),
            legend_label=f"{erc20_data.meta.symbol.upper()} balance of {short_address(address)}{self._get_right_axis_label(y_axis)}",
            **kwargs,
        )
        return self

    def with_erc20_total_supplies(
        self,
        token: str | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        erc20_data: ERC20Data | None = None,
        num_points: int = 100,
        **kwargs,
    ) -> DataView:
        if erc20_data is None and (start is None or end is None or token is None):
            raise ValueError(
                "erc20_data or all: start, end, and token should be specified"
            )
        if start is not None:
            erc20_data = ERC20Data.create(token, start, end)
        start = erc20_data.transfers["timestamp"][0]
        end = erc20_data.transfers["timestamp"][-1]
        step = (end - start) // num_points
        timestamps = [
            datetime.fromtimestamp(ts) for ts in range(start, end + step, step)
        ]

        balances = erc20_data.total_supplies(timestamps)["total_supply"].to_list()
        x_axis = XAxis.TIMESERIES
        y_axis = YAxis("Total Supply", erc20_data.meta.symbol.upper())
        self._update_axes(x_axis, y_axis, min(balances), max(balances))
        self._figure.line(
            timestamps,
            balances,
            color=self._get_color(),
            y_range_name=str(y_axis),
            line_width=2,
            legend_label=f"{erc20_data.meta.symbol.upper()} total supply{self._get_right_axis_label(y_axis)}",
            **kwargs,
        )
        return self

    def with_chainlink_prices(
        self,
        token0: str | None = None,
        token1: str | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        chainlink_data: ChainlinkData | None = None,
        num_points: int = 100,
        **kwargs,
    ) -> DataView:
        if chainlink_data is None and (
            start is None or end is None or token0 is None or token1 is None
        ):
            raise ValueError(
                "chainlink_data or all: start, end, token0 and token1 should be specified"
            )
        if start is not None:
            chainlink_data = ChainlinkData.create(token0, token1, start, end)
        start = chainlink_data.updates["timestamp"][0]
        end = chainlink_data.updates["timestamp"][-1]
        step = (end - start) // num_points
        timestamps = [datetime.fromtimestamp(ts) for ts in range(start, end, step)]

        prices = chainlink_data.prices(timestamps)["price"].to_list()
        x_axis = XAxis.TIMESERIES
        y_axis = YAxis(
            "Price",
            f"{chainlink_data.token0_meta.symbol} / {chainlink_data.token1_meta.symbol}",
        )
        self._update_axes(x_axis, y_axis, min(prices), max(prices))
        self._figure.line(
            timestamps,
            prices,
            color=self._get_color(),
            y_range_name=str(y_axis),
            line_width=2,
            legend_label=f"Price {chainlink_data.token0_meta.symbol.upper()} / {chainlink_data.token1_meta.symbol.upper()}{self._get_right_axis_label(y_axis)}",
            **kwargs,
        )
        return self

    def with_portfolio_by_token(
        self,
        tokens: List[str],
        addresses: List[str],
        base_token: str,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        num_points: int = 100,
        portfolio_data: PortfolioData | None = None,
        **kwargs,
    ):
        if portfolio_data is None and (
            start is None
            or end is None
            or tokens is None
            or addresses is None
            or base_token is None
        ):
            raise ValueError(
                "chainlink_data or all: start, end, token0 and token1 should be specified"
            )
        if start is not None:
            start, end = self._resolve_timestamps(start, end)
            step = (start - end) // num_points
            portfolio_data = PortfolioData.create(
                start=start,
                end=end,
                tokens=tokens,
                base_tokens=[base_token],
                addresses=addresses,
                step=step,
            )
        base_token_normalized = (
            portfolio_data._base_chainlink_datas[0]
            ._erc20_metas_service.get(base_token)
            .symbol
        )
        start = portfolio_data.data["timestamp"][0]
        end = portfolio_data.data["timestamp"][-1]
        step = (end - start) // num_points

        df = portfolio_data.breakdown_by_token(base_token)
        x_axis = XAxis.TIMESERIES
        y_axis = YAxis(
            "Balance",
            f"{base_token_normalized.upper()}",
        )
        self._update_axes(x_axis, y_axis, df["total"].min(), df["total"].max())
        num_series = len(portfolio_data._tokens)
        colors = Category20[20] * (1 + num_series // len(Category20[20]))
        colors = colors[:num_series]
        self._figure.varea_stack(
            stackers=portfolio_data._tokens,
            x="date",
            color=colors,
            legend_label=portfolio_data._tokens,
            source=df.to_dict(),
            alpha=0.8,
            **kwargs,
        )
        return self

    def show(self):
        show(self._figure)

    def _resolve_timestamps(self, timestamps: List[int | datetime]) -> List[int]:
        resolved = []
        for ts in timestamps:
            # resolve datetimes to timestamps
            if isinstance(ts, datetime):
                resolved.append(int(time.mktime(ts.timetuple())))
            else:
                resolved.append(ts)
        return resolved

    def _get_color(self):
        color = self._colors[self._numplots % len(self._colors)]
        self._numplots += 1
        return color

    def _get_right_axis_label(self, axis: YAxis) -> str:
        idx = self._y_axes.index(axis)
        return "" if idx % 2 == 0 else " (right)"

    def _update_axes(
        self, x_axis: XAxis, y_axis: YAxis, miny: float, maxy: float
    ) -> Dict[str, Any]:
        formatter = BasicTickFormatter()
        order = int(np.log((miny + maxy) / 2) / np.log(10))
        if order <= 0:
            zeroes = -order + 2
        if order > 0:
            zeroes = 3
        if order > 3:
            zeroes = 2
        if order > 6:
            zeroes = 3
        format = f"0.{''.join(['0'] * zeroes)}a"
        formatter = NumeralTickFormatter(format=format)

        y_axis_name = str(y_axis)
        defaults = {
            "toolbar_location": "above",
            "tools": "pan,wheel_zoom,hover,reset,save",
            "x_axis_type": x_axis.bokeh_type(),
            "height": 400,
            "width": int(400 * 2.3),
        }
        args = {**defaults, **self._fig_args}
        if not self._figure:
            self._x_axis = x_axis
            self._figure = figure(**args)
            self._figure.toolbar.autohide = True
            self._figure.extra_y_ranges = {y_axis_name: Range1d(miny, maxy)}
            self._figure.yaxis[0].y_range_name = y_axis_name
            self._figure.yaxis[0].axis_label = f"{y_axis.kind} ({y_axis.type})"
            self._figure.yaxis[0].formatter = formatter
            self._y_axes.append(y_axis)

        if self._x_axis != x_axis:
            raise ValueError(
                f"Tried to add plot with axis `{x_axis}` but the plot already have `{self._x_axis}` axis"
            )

        if y_axis_name in self._figure.extra_y_ranges:
            old_range = self._figure.extra_y_ranges[y_axis_name]
            new_range = Range1d(min(miny, old_range.start), max(maxy, old_range.end))
            self._figure.extra_y_ranges[y_axis_name] = new_range
        else:
            self._y_axes.append(y_axis)
            self._figure.extra_y_ranges[y_axis_name] = Range1d(miny, maxy)
            loc = "left" if len(self._figure.yaxis) % 2 == 0 else "right"
            self._figure.add_layout(
                LinearAxis(
                    y_range_name=y_axis_name,
                    axis_label=f"{y_axis.kind} ({y_axis.type})",
                    formatter=formatter,
                ),
                loc,
            )

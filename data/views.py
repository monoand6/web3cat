from __future__ import annotations
from datetime import datetime
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
from bokeh.palettes import Category10, Pastel1
from data.chainlink.chainlink_data import ChainlinkData
from data.erc20s.erc20_data import ERC20Data
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

    def show(self):
        show(self._figure)

    def _get_color(self):
        color = self._colors[self._numplots % len(self._colors)]
        self._numplots += 1
        return color

    def _get_right_axis_label(self, axis: YAxis) -> str:
        range_names = [a.y_range_name for a in self._figure.yaxis]
        idx = range_names.index(str(axis))
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

        if self._x_axis != x_axis:
            raise ValueError(
                f"Tried to add plot with axis `{x_axis}` but the plot already have `{self._x_axis}` axis"
            )

        if y_axis_name in self._figure.extra_y_ranges:
            old_range = self._figure.extra_y_ranges[y_axis_name]
            new_range = Range1d(min(miny, old_range.start), max(maxy, old_range.end))
            self._figure.extra_y_ranges[y_axis_name] = new_range
        else:
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

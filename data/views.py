from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Set
from bokeh.plotting import Figure, figure, show
from bokeh.models import Range1d, LinearAxis, DatetimeAxis
from data.erc20s.erc20_data import ERC20Data
from fetcher.blocks.service import DEFAULT_BLOCK_TIMESTAMP_GRID


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
    _y_axes: Set(YAxis)
    _fig_args: Dict[str, Any]

    def __init__(self, **kwargs):
        self._fig_args = kwargs
        self._figure = None
        self._x_axis = None
        self._y_axes = []

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
        y_axis = YAxis("balance", erc20_data.meta.symbol)
        axis_options = self._init_axes(x_axis, y_axis)
        self._figure.line(timestamps, balances, **axis_options)
        return self

    def show(self):
        show(self._figure)

    def _init_axes(self, x_axis: XAxis, y_axis: YAxis) -> Dict[str, Any]:
        defaults = {
            "toolbar_location": "above",
            "tools": "pan,wheel_zoom,reset",
            "x_axis_type": x_axis.bokeh_type(),
            "height": 400,
            "width": int(400 * 1.618),
        }
        args = {**defaults, **self._fig_args}
        if not self._figure:
            self._x_axis = x_axis
            self._figure = figure(**args)
            self._figure.toolbar.autohide = True
            self._figure.extra_y_ranges = {}

        if self._x_axis != x_axis:
            raise ValueError(
                f"Tried to add plot with axis `{x_axis}` but the plot already have `{self._x_axis}` axis"
            )

        y_axis_name = str(y_axis)
        if not y_axis in self._y_axes:
            self._y_axes.append(y_axis)
            if len(self._y_axes) > 1:
                loc = "left" if len(self._y_axes) % 2 == 1 else "right"
                self._figure.extra_y_ranges[y_axis_name] = Range1d()
                self._figure.add_layout(LinearAxis(y_range_name=y_axis_name), loc)

        if y_axis == self._y_axes[0]:
            return {}
        return {"y_range_name": str(y_axis)}

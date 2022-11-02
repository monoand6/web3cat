from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any, Dict, List
import time
from bokeh.plotting import Figure, show
from bokeh.models import (
    BasicTickFormatter,
    NumeralTickFormatter,
    Range1d,
    LinearAxis,
    GlyphRenderer,
)
from bokeh.plotting import figure
import numpy as np

from data.erc20s.erc20_data import ERC20Data
from fetcher.erc20_metas import ERC20Meta, ERC20MetasService


@dataclass(frozen=True)
class Wireframe:
    numpoints: int

    @property
    def data_key(self):
        pass

    @property
    def x_axis(self):
        pass

    @property
    def y_axis(self):
        pass

    def x(self, data: Any) -> List[Any]:
        pass

    def y(self, data: Any) -> List[Any]:
        pass

    def build_data(self, default_data: Any | None, **core_args) -> Any:
        pass

    def plot(self, fig: Figure, x: Any, y: Any, **kwargs) -> GlyphRenderer:
        pass


@dataclass(frozen=True)
class TimeseriesWireframe(Wireframe):
    start: int | datetime
    end: int | datetime

    @property
    def x_axis(self) -> str:
        return "datetime"

    def x(self, data: Any):
        start = self._resolve_datetime(self.start)
        end = self._resolve_datetime(self.end)
        step = (end - start) // self.numpoints
        timestamps = [z for z in range(start, end, step)]
        if timestamps[-1] != end:
            timestamps.append(end)
        dates = [datetime.fromtimestamp(t) for t in timestamps]
        return dates

    def _resolve_datetime(self, tim: int | datetime):
        if isinstance(tim, datetime):
            return int(time.mktime(tim.timetuple()))
        return tim


@dataclass(frozen=True)
class TotalSupplyWireframe(TimeseriesWireframe):
    token: ERC20Meta

    @cached_property
    def data_key(self) -> str:
        return self.token.symbol.upper()

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
            **kwargs,
        )


class View:
    _wireframes: List[Wireframe]
    _core_args: Dict[str, Any]
    _fig_args: Dict[str, Any]
    _defaults: Dict[str, Any]

    _erc20_metas_service: ERC20MetasService
    _datas: Dict[str, Any]

    def __init__(self, **kwargs):
        self._wireframes = []
        self._datas = {}
        self._core_args = {
            k: kwargs.pop(k)
            for k in ["rpc", "cache_path", "block_grid_step", "w3", "conn"]
            if k in kwargs
        }
        self._fig_args = {
            k: kwargs.pop(k)
            for k in [
                "x_range",
                "y_range",
                "x_axis_type",
                "y_axis_type",
                "tools",
                "x_minor_ticks",
                "y_minor_ticks",
                "x_axis_location",
                "y_axis_location",
                "x_axis_label",
                "y_axis_label",
                "active_drag",
                "active_inspect",
                "active_scroll",
                "active_tap",
                "active_multi",
                "tooltips",
            ]
            if k in kwargs
        }
        self._erc20_metas_service = ERC20MetasService.create(**self._core_args)
        self._defaults = {"numpoints": 100, **kwargs}

    @cached_property
    def figure(self):
        return figure(
            toolbar_location="above",
            tools="pan,wheel_zoom,hover,reset,save",
            x_axis_type=self._wireframes[0].x_axis,
            height=400,
            width=int(400 * 2.3),
            **self._fig_args,
        )

    def total_supply(
        self,
        token: str,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        numpoints: int | None = None,
    ):
        token_meta = self._erc20_metas_service.get(token)
        self._wireframes.append(
            TotalSupplyWireframe(
                **self._build_wireframe_args(
                    {
                        "token": token_meta,
                        "start": start,
                        "end": end,
                        "numpoints": numpoints,
                    }
                )
            )
        )
        return self

    def show(self):
        self._build_data()
        for wf in self._wireframes:
            data = self._datas[wf.data_key]
            x = wf.x(data)
            y = wf.y(data)
            self._update_axes(self.figure, min(y), max(y), wf)
            wf.plot(self.figure, x, y)
        show(self.figure)

    def _build_data(self):
        for wf in self._wireframes:
            data = self._datas.get(wf.data_key, None)
            self._datas[wf.data_key] = wf.build_data(data, **self._core_args)

    def _build_wireframe_args(self, args: Dict[str, Any]):
        clean_args = {k: v for k, v in args.items() if not v is None}
        merged = {**self._defaults, **clean_args}
        return {k: v for k, v in merged.items() if k in args}

    def _update_axes(
        self, fig: Figure, miny: float, maxy: float, wf: Wireframe
    ) -> Dict[str, Any]:
        y_axis = wf.y_axis
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
        fmt = f"0.{''.join(['0'] * zeroes)}a"
        formatter = NumeralTickFormatter(format=fmt)

        fig.extra_y_ranges = fig.extra_y_ranges or {}
        needs_y_axis_init = len(fig.extra_y_ranges.keys()) == 0
        needs_y_axis_add = not y_axis in fig.extra_y_ranges
        old_range = fig.extra_y_ranges.get(y_axis, Range1d(miny, maxy))
        new_range = Range1d(min(miny, old_range.start), max(maxy, old_range.end))
        fig.extra_y_ranges[wf.y_axis] = new_range

        if needs_y_axis_init:
            fig.yaxis[0].y_range_name = y_axis
            fig.yaxis[0].axis_label = y_axis
            fig.yaxis[0].formatter = formatter
        elif needs_y_axis_add:
            loc = "left" if len(fig.yaxis) % 2 == 0 else "right"
            fig.add_layout(
                LinearAxis(
                    y_range_name=y_axis,
                    axis_label=y_axis,
                    formatter=formatter,
                ),
                loc,
            )

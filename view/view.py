from datetime import datetime
from functools import cached_property
from typing import Any, Dict, List
from bokeh.plotting import Figure, show
from bokeh.models import (
    BasicTickFormatter,
    NumeralTickFormatter,
    Range1d,
    LinearAxis,
)
from bokeh.plotting import figure
from bokeh.palettes import Category10
import numpy as np
from web3.constants import ADDRESS_ZERO

from fetcher.erc20_metas import ERC20MetasService, ERC20Meta
from view.wireframes import (
    Wireframe,
    BalanceWireframe,
    TotalSupplyWireframe,
    ChainlinkPricesWireframe,
    EthBalanceWireframe,
    PortfolioByAddressWireframe,
    PortfolioByTokenWireframe,
)


class View:
    _wireframes: List[Wireframe]
    _core_args: Dict[str, Any]
    _fig_args: Dict[str, Any]
    _defaults: Dict[str, Any]
    _colors: List[Any]

    _erc20_metas_service: ERC20MetasService
    _glyphs: List[Any]
    _datas: Dict[str, Any]

    def __init__(self, **kwargs):
        self._wireframes = []
        self._datas = {}
        self._colors = kwargs.pop("colors", Category10[10])
        self._glyphs = []
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

    def get_data(self, index: int) -> Any:
        return self._datas[self._wireframes[index].data_key]

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
        token: str | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        numpoints: int | None = None,
    ):
        args = self._build_wireframe_args(
            {
                "token": token,
                "start": start,
                "end": end,
                "numpoints": numpoints,
            }
        )
        args["token"] = self._erc20_metas_service.get(args["token"])
        self._wireframes.append(TotalSupplyWireframe(**args))
        return self

    def chainlink_prices(
        self,
        token: str | None = None,
        token_base: str | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        numpoints: int | None = None,
    ):
        args = self._build_wireframe_args(
            {
                "token": token,
                "token_base": token_base,
                "start": start,
                "end": end,
                "numpoints": numpoints,
            }
        )
        for name in ["token", "token_base"]:
            if args[name].upper() == "USD":
                args[name] = ERC20Meta(1, ADDRESS_ZERO, "USD", "USD", 6, None)
            else:
                args[name] = self._erc20_metas_service.get(args[name])
        args["token0"] = args.pop("token")
        args["token1"] = args.pop("token_base")
        self._wireframes.append(ChainlinkPricesWireframe(**args))
        return self

    def balance(
        self,
        address: str | List[str] | None = None,
        token: str | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        numpoints: int | None = None,
    ):
        args = self._build_wireframe_args(
            {
                "token": token,
                "address": address,
                "start": start,
                "end": end,
                "numpoints": numpoints,
            }
        )
        if isinstance(args["address"], str):
            args["address"] = [args["address"]]
        for addr in args["address"]:
            args_item = {**args, "address": addr}
            if args_item["token"].upper() == "ETH":
                args_item.pop("token")
                self._wireframes.append(EthBalanceWireframe(**args_item))
            else:
                args_item["token"] = self._erc20_metas_service.get(args_item["token"])
                self._wireframes.append(BalanceWireframe(**args_item))
        return self

    def portfolio_by_address(
        self,
        tokens: List[str] | None = None,
        base_token: str | None = None,
        addresses: List[str] | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        numpoints: int | None = None,
    ):
        args = self._build_wireframe_args(
            {
                "tokens": tokens,
                "base_token": base_token,
                "addresses": addresses,
                "start": start,
                "end": end,
                "numpoints": numpoints,
            }
        )
        token_metas = [self._erc20_metas_service.get(token) for token in args["tokens"]]
        base_token_meta = self._erc20_metas_service.get(args["base_token"])
        addresses = [a.lower() for a in (args["addresses"] or [])]
        args["tokens"] = token_metas
        args["base_token"] = base_token_meta
        args["addresses"] = addresses

        self._wireframes.append(PortfolioByAddressWireframe(**args))

        return self

    def portfolio_by_token(
        self,
        tokens: List[str] | None = None,
        base_token: str | None = None,
        addresses: List[str] | None = None,
        start: int | datetime | None = None,
        end: int | datetime | None = None,
        numpoints: int | None = None,
    ):
        args = self._build_wireframe_args(
            {
                "tokens": tokens,
                "base_token": base_token,
                "addresses": addresses,
                "start": start,
                "end": end,
                "numpoints": numpoints,
            }
        )
        token_metas = [self._erc20_metas_service.get(token) for token in args["tokens"]]
        base_token_meta = self._erc20_metas_service.get(args["base_token"])
        addresses = [a.lower() for a in (args["addresses"] or [])]
        args["tokens"] = token_metas
        args["base_token"] = base_token_meta
        args["addresses"] = addresses

        self._wireframes.append(PortfolioByTokenWireframe(**args))

        return self

    def show(self):
        self._build_data()
        for wf in self._wireframes:
            data = self._datas[wf.data_key]
            x = wf.x(data)
            y = wf.y(data)
            if isinstance(y, dict):
                for y_sub in y.values():
                    self._update_axes(self.figure, min(y_sub), max(y_sub), wf)
            else:
                if len(y) > 0:
                    if isinstance(y[0], list):
                        for y_sub in y:
                            self._update_axes(self.figure, min(y_sub), max(y_sub), wf)
                    else:
                        self._update_axes(self.figure, min(y), max(y), wf)

            self._glyphs.append(
                wf.plot(
                    self.figure, x, y, color=self._get_color(), y_range_name=wf.y_axis
                )
            )
        show(self.figure)

    def _build_data(self):
        for wf in self._wireframes:
            data = self._datas.get(wf.data_key, None)
            self._datas[wf.data_key] = wf.build_data(data, **self._core_args)

    def _build_wireframe_args(self, args: Dict[str, Any]):
        clean_args = {k: v for k, v in args.items() if not v is None}
        merged = {**self._defaults, **clean_args}
        return {k: v for k, v in merged.items() if k in args}

    def _get_color(self):
        color = self._colors[len(self._glyphs) % len(self._colors)]
        return color

    def _update_axes(
        self, fig: Figure, miny: float, maxy: float, wf: Wireframe
    ) -> Dict[str, Any]:
        miny /= 1.05
        maxy *= 1.05
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

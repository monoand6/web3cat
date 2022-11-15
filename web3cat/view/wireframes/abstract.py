from typing import List, Any
from datetime import datetime
import time
from dataclasses import dataclass
from bokeh.plotting import Figure
from bokeh.models import GlyphRenderer


@dataclass(frozen=True, kw_only=True)
class Wireframe:
    """
    Base class for all wireframes. Wireframe is a set of arguments
    that are passed to build a plot. Then it has the logic of
    transforming these arguments into the plot itself, including
    building the underlying dataset.
    """

    numpoints: int
    x_axis_name: str | None = None
    y_axis_name: str | None = None

    @property
    def data_key(self):
        """
        The underlying dataset is considered the same for the same data_key.
        For example it's good to have the same datakey for the same ERC20 token.
        """

    @property
    def x_axis(self):
        """
        The bokeh type for the x axis
        """
        raise RuntimeError("Not implemented")

    @property
    def y_axis(self):
        """
        The name of the axis. Data with the same axis name will
        be plotted on the same axis.
        """
        raise RuntimeError("Not implemented")

    def x(self, data: Any) -> List[Any]:
        """
        A list of x values for the plot.

        Arguments:
            data: Underlying dataset

        Returns:
            A list of x values
        """
        raise RuntimeError("Not implemented")

    def y(self, data: Any) -> List[Any]:
        """
        A list of y values for the plot.

        Arguments:
            data: Underlying dataset

        Returns:
            A list of y values
        """
        raise RuntimeError("Not implemented")

    def build_data(self, default_data: Any | None, **core_args) -> Any:
        """
        Build data from the wireframe arguments.

        Arguments:
            default_data: A data with the same data_key that was built
                          from other wireframes.

        Returns:
            An updated or created dataset.
        """
        raise RuntimeError("Not implemented")

    def plot(self, fig: Figure, x: Any, y: Any, **kwargs) -> GlyphRenderer:
        """
        Make a plot on figure.

        Arguments:
            fig: :class:`bokeh.plotting.Figure` for the plot
            x: A list of x values
            y: A list of y values

        Returns:
            A plot
        """
        raise RuntimeError("Not implemented")


@dataclass(frozen=True)
class TimeseriesWireframe(Wireframe):
    """
    An abstract class for any timeseries wireframe
    """

    #: Start of the timeseries
    start: int | datetime
    #: End of the timeseries
    end: int | datetime

    @property
    def x_axis(self) -> str:
        return "datetime"

    def x(self, data: Any):
        start = self._resolve_datetime(self.start)
        end = self._resolve_datetime(self.end)
        step = (end - start) // (self.numpoints - 1)
        timestamps = [*range(start, end, step)]
        if timestamps[-1] != end:
            timestamps.append(end)
        dates = [datetime.fromtimestamp(t) for t in timestamps]
        return dates

    def _resolve_datetime(self, tim: int | datetime):
        if isinstance(tim, datetime):
            return int(time.mktime(tim.timetuple()))
        return tim

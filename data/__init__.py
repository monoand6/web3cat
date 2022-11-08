# pylint: disable=line-too-long
"""
Module for working with web3 datasets.

The datasets are fetched from blockchains via RPC endpoint, cached
and returned as a `polars.Dataframe <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html>`_.

Note:
    Throughout the docs a notion of ``timepoint`` is used.
    ``timepoint`` is either block number or Unix timestamp or datetime.

Note:
    All values in the datasets are in natural units. For example ETHs,
    not WEIs.
"""

import polars as pl
from bokeh.io import curdoc

from data.erc20s import ERC20Data
from data.chainlink.chainlink_data import ChainlinkUSDData, ChainlinkData
from data.ethers import EtherData
from data.portfolios import PortfolioData


pl.Config.set_fmt_str_lengths(44)
curdoc().theme = "dark_minimal"

import polars as pl
from bokeh.io import curdoc

pl.Config.set_fmt_str_lengths(44)
curdoc().theme = "dark_minimal"

from data.erc20s import ERC20Data
from data.chainlink.chainlink_data import ChainlinkUSDData, ChainlinkData
from data.ethers import EtherData
from data.portfolios import PortfolioData

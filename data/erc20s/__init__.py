"""
Fetches ERC20 datasets and returns them as
`polars.Dataframe <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html>`_.

Using :class:`ERC20Data` you can:

#. Get the timeseries of the wallet balances
#. Get all transfers for the wallet
#. Get top volumes breakdown by the wallet
#. etc.

Examples:

    **1. Get volumes breakdown by the wallet for 1 day**


    .. code::

        rpc = "https://eth-mainnet.g.alchemy.com/v2/<your_key>"
        data = ERC20Data.create("DAI", datetime(2022, 9, 1), datetime(2022, 9, 2), rpc=rpc)
        data.volumes
    
    **2. Get balance timeseries of a 0.01% DAI-USDC pool**

    .. code::

        rpc = "https://eth-mainnet.g.alchemy.com/v2/<your_key>"
        pool = "0x5777d92f208679db4b9778590fa3cab3ac9e2168"
        data = ERC20Data.create("DAI", datetime(2022, 6, 1), datetime(2022, 9, 1), address_filter=[pool], rpc=rpc)
        data.balances(pool_address, [datetime(2022, 6, 1), datetime(2022, 7, 1), datetime(2022, 8, 1), datetime(2022, 9, 1)])

        
    
    **3. Get all transactions for a wallet**

    .. code::

        import polars as pl
        rpc = "https://eth-mainnet.g.alchemy.com/v2/<your_key>"
        wallet = "0x5777d92f208679db4b9778590fa3cab3ac9e2168"
        wallets = [wallet, "0x50379f632ca68d36e50cfbc8f78fe16bd1499d1e", "0xfdc0569229a0647ce7db39657543ce23bc970c0b"]

        # Only fetch data for 3 specific wallets 
        # DAI data for all wallets for 3 months would be huge
        # It will take considerable time to download it
        data = ERC20Data.create("DAI", datetime(2022, 6, 1), datetime(2022, 9, 1), address_filter=wallets, rpc=rpc)

        # Filter fetched data only for one wallet 
        # Data is already fetched -> it's in-memory operation using polars
        data.transfers.filter((pl.col("from") == wallet) | (pl.col("to") == wallet))

"""

from data.erc20s.erc20_data import ERC20Data

"""
Fetches Gearbox data and returns it as
`polars.Dataframe <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe.html>`_.

Using :class:`GearboxData` you can:

#. Get the list of liquidation events
#. Get dynamic health factor for the position

Examples:

    .. code::

        from datetime import datetime
        from web3cat.data import GearboxData

        data = GearboxData(start = datetime(2022, 1, 1), end = datetime(2022, 12, 12))

        # List all liquidations
        data.liquidations

"""


from web3cat.data.gearbox.gearbox_data import GearboxData

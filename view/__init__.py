"""
Visualization for blockchain data.

The workflow for the visualization:

    1. Fetch the data using classes from :mod:`data`
    2. Visualize the data using `bokeh <https://bokeh.org>`_ lib

All data is cached and can be accessed from the :class:`View`

Examples:

    **Preliminary setup**

    .. code::

        from datetime import datetime
        from bokeh.io import output_notebook
        from view import View

        # for python notebooks
        output_notebook()

    **Example 1**

    .. code::

        v1 = View(token="DAI", start=datetime(2022, 6, 1), end = datetime(2022, 10, 30)) \
            .total_supply() \
            .balance(["0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643", "0x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65"])
        v1.show()

    .. image:: images/view1.png

    **Example 2**
    
    .. code::

        v2 = View(start=datetime(2022, 6, 1), end = datetime(2022, 10, 30)) \
            .total_supply(token="USDC") \
            .total_supply(token="DAI")
        v2.show()

    .. image:: images/view2.png

    **Example 3**
    
    .. code::

        v3 = View(token="WBTC", start=datetime(2022, 6, 1), end = datetime(2022, 10, 30)) \
            .total_supply() \
            .chainlink_prices(token_base = "USDC")
        v3.show()

    .. image:: images/view3.png

    **Example 4**
    
    .. code::

        v4 = View(token="ETH", start=datetime(2022, 6, 1), end = datetime(2022, 10, 30)) \
            .balance(["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "0xf977814e90da44bfa03b6295a0616a897441acec"])
        v4.show()

    .. image:: images/view4.png

    **Example 5**
    
    .. code::

        addresses = [
            "0x78605Df79524164911C144801f41e9811B7DB73D",
            "0xBF72Da2Bd84c5170618Fbe5914B0ECA9638d5eb5",
            "0x8EB8a3b98659Cce290402893d0123abb75E3ab28"
        ]
        tokens = ["USDC", "WETH", "ETH", "WBTC"]
        base_tokens = ["USDC", "WETH"]

        v5 = View().portfolio_by_token(
            addresses = addresses, 
            tokens = tokens, 
            base_token = "USDC", 
            start=datetime(2022, 10, 1), 
            end=datetime(2022, 10, 30), 
            numpoints=100
        )
        v5.show()

        v6 = View().portfolio_by_address(
            addresses = addresses, 
            tokens = tokens, 
            base_token = "USDC", 
            start=datetime(2022, 10, 1), 
            end=datetime(2022, 10, 30), 
            numpoints=100
        )
        v6.show()

    .. image:: images/view5.png
    .. image:: images/view6.png

    **Example 6**
    
    .. code::

        v6.get_data(0).breakdown_by_token("USDC")

    .. image:: images/view7.png
"""

from view.view import View

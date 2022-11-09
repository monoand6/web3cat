Getting started
===============

1. Install python package 

.. code::

    pip install web3cat

2. Setup your archive node rpc. The easiest and free way is to use
   `Alchemy <https://alchemy.com>`_.

3. Set initial configuration

.. code::

    import os
    os.environ['WEB3_PROVIDER_URI'] = 'https://eth-mainnet.g.alchemy.com/v2/<YOUR_ALCHEMY_API_KEY>'
    os.environ['WEB3_CACHE_PATH']="cache.sqlite3"

4. (optional for Jupyter) Initialize bokeh for python notebooks

.. code::

    from bokeh.io import output_notebook

    output_notebook()

5. Run sample visualization

.. code::

    from web3cat.view import View
    from datetime import datetime

    v = View(token="DAI", start=datetime(2022, 6, 1), end = datetime(2022, 10, 30)) \
        .total_supply() \
        .balance(["0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643", "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"])
    v.show()

.. image:: images/view1.png

6. Get underlying data

    .. code::

        v.get_data(0).transfers[["date", "block_number", "from", "to", "value"]]

    .. image:: images/view_getting_started1.png

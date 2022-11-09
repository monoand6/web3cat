Technical details
=================

There are 3 layers in the framework that could be used completely
independent of each other:

#. :mod:`view` - visualize blockchain data (token supply, balances, prices, ...)
#. :mod:`data` - work with blockchain data (filter, join, export to csv, ...)
#. :mod:`fetcher` - fetch and cache raw blockchain data (events, function calls, balances, ...)

Behind each layer there's a powerful opensource Python library

.. image:: /images/web3cat_arch.png
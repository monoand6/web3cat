"""
Data for analyzing a portfolio of different tokens.

Examples:
    .. code::

        from datetime import datetime
        from web3cat.data import PortfolioData

        addresses = [
            "0x78605Df79524164911C144801f41e9811B7DB73D",
            "0xBF72Da2Bd84c5170618Fbe5914B0ECA9638d5eb5",
            "0x8EB8a3b98659Cce290402893d0123abb75E3ab28"
        ]
        tokens = ["USDC", "WETH", "ETH", "WBTC"]
        base_tokens = ["USDC", "WETH"]
        portfolio_data = PortfolioData(
            tokens=tokens,
            base_tokens=base_tokens,
            addresses = addresses,
            start = datetime(2022, 6, 1),
            end = datetime(2022, 9, 1),
            numpoints = 4
        )

        # Portfolio value in USDC by address for USDC, WETH, ETH and WBTC holdings
        portfolio_data.breakdown_by_address("USDC")

        # Portfolio value in WETH by address for USDC, WETH, ETH and WBTC holdings
        portfolio_data.breakdown_by_address("WETH")

        # Portfolio value in USDC by token for USDC, WETH, ETH and WBTC holdings
        portfolio_data.breakdown_by_token("USDC")

        # Portfolio value in WETH by token for USDC, WETH, ETH and WBTC holdings
        portfolio_data.breakdown_by_token("WETH")

        # Balance and price data
        portfolio_data.balances_and_prices

        # Balances of a one specific token
        portfolio_data.balances("USDC")
"""

from web3cat.data.portfolios.portfolio import PortfolioData

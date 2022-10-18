from string import ascii_letters
from hypothesis.strategies import integers, text, dictionaries, builds, SearchStrategy
from fetcher.balances.balance import Balance


def balance() -> SearchStrategy[Balance]:
    return builds(
        Balance,
        integers(0, 10000),
        integers(0, 10000),
        text(ascii_letters),
        integers(0, 10000),
    )

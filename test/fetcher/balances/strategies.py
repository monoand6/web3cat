from string import ascii_letters
from hypothesis.strategies import (
    integers,
    text,
    dictionaries,
    builds,
    SearchStrategy,
    just,
)
from web3cat.fetcher.balances.balance import Balance


def balance() -> SearchStrategy[Balance]:
    return builds(
        Balance,
        just(1),
        integers(0, 10000),
        text(ascii_letters),
        integers(0, 10000),
    )

from string import ascii_letters
from hypothesis.strategies import (
    integers,
    text,
    dictionaries,
    builds,
    SearchStrategy,
    just,
)
from web3cat.fetcher.calls.call import Call


def call() -> SearchStrategy[Call]:
    return builds(
        Call,
        just(1),
        text(ascii_letters),
        text(ascii_letters),
        integers(0, 10000),
        dictionaries(text(ascii_letters), integers()),
    )

from string import ascii_letters
from hypothesis.strategies import (
    integers,
    text,
    dictionaries,
    builds,
    SearchStrategy,
    just,
)
from web3cat.fetcher.erc20_metas.erc20_meta import ERC20Meta


def erc20_meta() -> SearchStrategy[ERC20Meta]:
    return builds(
        ERC20Meta,
        just(1),
        text(ascii_letters),
        text(ascii_letters),
        text(ascii_letters),
        integers(0, 18),
        just(None),
    )

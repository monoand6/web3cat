from string import ascii_letters
from hypothesis.strategies import integers, text, dictionaries, builds, SearchStrategy
from probe.erc20_metas.erc20_meta import ERC20Meta


def erc20_meta() -> SearchStrategy[ERC20Meta]:
    return builds(
        ERC20Meta,
        integers(0, 10000),
        text(ascii_letters),
        text(ascii_letters),
        text(ascii_letters),
        integers(0, 18),
    )

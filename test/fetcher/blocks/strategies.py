from string import ascii_letters
from hypothesis.strategies import integers, text, dictionaries, builds, SearchStrategy
from fetcher.blocks.block import Block


def block() -> SearchStrategy[Block]:
    return builds(
        Block,
        integers(0, 10000),
        integers(0, 10000),
        integers(100_000, 1_000_000),
    )

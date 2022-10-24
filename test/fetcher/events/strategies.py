from string import ascii_letters
from hypothesis.strategies import (
    integers,
    text,
    dictionaries,
    builds,
    SearchStrategy,
    just,
)
from fetcher.events.event import Event


def event() -> SearchStrategy[Event]:
    return builds(
        Event,
        just(1),
        integers(0, 10000),
        text(ascii_letters),
        integers(0, 10000),
        text(ascii_letters),
        text(ascii_letters),
        dictionaries(text(ascii_letters), integers()),
    )

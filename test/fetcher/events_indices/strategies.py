from string import ascii_letters, printable
from typing import Any, Dict, Tuple
from hypothesis.strategies import composite, integers, text, lists, one_of
from web3cat.fetcher.events_indices.index import EventsIndexData, EventsIndex


@composite
def args_subset_and_superset(draw) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    res1 = {}
    res2 = {}
    for k in draw(lists(text(printable))):
        res1[k] = draw(
            one_of(
                lists(one_of(integers(), text(printable))), integers(), text(printable)
            )
        )
        res2[k] = res1[k]
        if isinstance(res1[k], list):
            res1[k] = res2[k] + draw(lists(one_of(integers(), text(printable))))
    for k in draw(lists(text(printable))):
        if k in res1.keys():
            continue
        res2[k] = draw(
            one_of(
                lists(one_of(integers(), text(printable))), integers(), text(printable)
            )
        )
    return (res1, res2)


@composite
def events_index(draw) -> EventsIndex:
    args = {}
    for k in draw(lists(text(ascii_letters))):
        args[k] = draw(
            one_of(
                lists(integers()),
                lists(text(ascii_letters)),
                integers(),
                text(ascii_letters),
            )
        )
    data = EventsIndexData()
    for _ in range(draw(integers(0, 20))):
        start = draw(integers(0, 1_000_000))
        end = start + draw(integers(0, 200_000))
        start = data.snap_block_to_grid(start)
        end = data.snap_block_to_grid(end)
        data.set_range(start, end, True)
    return EventsIndex(
        draw(integers()),
        draw(text(ascii_letters)),
        draw(text(ascii_letters)),
        args,
        data,
    )

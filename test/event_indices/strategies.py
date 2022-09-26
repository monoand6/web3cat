from string import printable
from typing import Any, Dict, Tuple
from hypothesis.strategies import composite, integers, text, lists, one_of
from probe.event_indices.index import EventsIndexData, EventsIndex


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
        if isinstance(res1[k], list):
            res2[k] = res1[k] + draw(lists(one_of(integers(), text(printable))))
        else:
            res2[k] = res1[k]
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
    for k in draw(lists(text(printable))):
        args[k] = draw(
            one_of(
                lists(one_of(integers(), text(printable))), integers(), text(printable)
            )
        )
    data = EventsIndexData(draw(integers(8, 1000)) * 86400)
    for _ in range(draw(integers(0, 20))):
        start = draw(integers(8, 1000)) * 86400
        end = start + draw(integers(0, 200)) * 86400
        data.set_range(start, end, True)
    return EventsIndex(
        draw(integers()), draw(text(printable)), draw(text(printable)), args, data
    )

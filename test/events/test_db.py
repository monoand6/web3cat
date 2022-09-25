from string import printable
from typing import Any, Dict, Tuple
from hypothesis import given
from hypothesis.strategies import composite, integers, text, lists, one_of
from probe.events.db import args_is_subset


@composite
def args_subset_and_superset(draw):
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


@given(args_subset_and_superset())
def test_args_is_subset(sub_and_sup: Tuple[Dict[str, Any], Dict[str, Any]]):
    sub, sup = sub_and_sup
    assert args_is_subset(sub, sup)


def test_args_is_subset_edge():
    assert args_is_subset(None, None)
    assert args_is_subset(None, {})
    assert args_is_subset({}, {})
    assert args_is_subset({}, None)
    assert not args_is_subset({"x": 1}, None)
    assert args_is_subset(None, {"x": 1})
    assert args_is_subset({}, {"x": 1})

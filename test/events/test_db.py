from typing import Any, Dict, Tuple
from hypothesis import given

from probe.events.db import args_is_subset
from events.strategies import args_subset_and_superset


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

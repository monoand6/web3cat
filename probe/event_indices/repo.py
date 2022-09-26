from typing import Any


def args_is_subset(subset: Any | None, superset: Any | None) -> bool:
    if subset is None:
        return True
    if superset is None:
        if isinstance(subset, dict) and len(subset.keys()) == 0:
            return True
        return False
    if isinstance(subset, dict):
        if not isinstance(superset, dict):
            return False
        for key in subset.keys():
            if not key in superset:
                return False
            if not args_is_subset(subset[key], superset[key]):
                return False
        return True
    if isinstance(subset, list):
        if not isinstance(superset, list):
            return False
        sb = set(subset)
        sp = set(superset)
        return sb.issubset(sp)
    return subset == superset

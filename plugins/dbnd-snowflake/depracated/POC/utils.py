from collections import defaultdict
from typing import Union


def ddict2dict(d: Union[defaultdict, dict]) -> dict:
    """convert defaultdict into a dict"""
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = ddict2dict(v)
    return dict(d)

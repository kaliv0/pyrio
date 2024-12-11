from collections.abc import Mapping
from functools import wraps

from pyrio.utils import DictItem


def map_dict_items(func):
    @wraps(func)
    def wrapper(*args, **kw):
        if not any(isinstance(arg, Mapping) for arg in args):
            return func(*args, **kw)

        remapped = []
        for arg in args:
            if isinstance(arg, Mapping):
                remapped.append((DictItem(k, v) for k, v in arg.items()))
            else:
                remapped.append(arg)
        return func(*remapped, **kw)

    return wrapper

from collections.abc import Mapping
from functools import wraps

from pyrio.utils.dict_item import Item
from pyrio.utils.exception import IllegalStateError

TERMINAL_FUNCTIONS = [
    "for_each",
    "reduce",
    "count",
    "sum",
    "find_first",
    "find_any",
    "any_match",
    "all_match",
    "none_match",
    "min",
    "max",
    "compare_with",
    "to_list",
    "to_tuple",
    "to_set",
    "to_dict",
    "group_by",
    "take_nth",
    "all_equal",
]


def pre_call(function_decorator):
    def decorator(cls):
        for name, obj in vars(cls).items():
            if callable(obj):
                setattr(cls, name, function_decorator(obj))
        return cls

    return decorator


def handle_consumed(func):
    @wraps(func)
    def wrapper(*args, **kw):
        from pyrio.streams.base_stream import BaseStream

        if not args or not isinstance(args[0], BaseStream):
            return func(*args, **kw)

        is_consumed = getattr(args[0], "_is_consumed", None)
        if is_consumed:
            raise IllegalStateError("Stream object already consumed")

        result = func(*args, **kw)
        if is_consumed is False and func.__name__ in TERMINAL_FUNCTIONS:
            args[0]._is_consumed = True  # noqa
        return result

    return wrapper


def map_dict_items(func):
    @wraps(func)
    def wrapper(*args, **kw):
        if not any(isinstance(arg, Mapping) for arg in args):
            return func(*args, **kw)

        remapped = []
        for arg in args:
            if isinstance(arg, Mapping):
                remapped.append((Item(k, v) for k, v in arg.items()))
            else:
                remapped.append(arg)
        return func(*remapped, **kw)

    return wrapper

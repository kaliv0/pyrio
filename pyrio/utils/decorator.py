from collections.abc import Mapping
from functools import wraps

from pyrio.utils.dict_item import DictItem
from pyrio.utils.exception import IllegalStateError

TERMINAL_FUNCTIONS = [
    "for_each",
    "reduce",
    "count",
    "sum",
    "min",
    "max",
    "find_first",
    "find_any",
    "take_first",
    "take_last",
    "take_nth",
    "any_match",
    "all_match",
    "none_match",
    "compare_with",
    "all_equal",
    "quantify",
    "group_by",
    "collect",
    "to_list",
    "to_tuple",
    "to_set",
    "to_dict",
    "to_string",
    "save",
    # "close"  # TODO: only used manually, no reason to be here
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

        stream = args[0] if args else None
        if not (stream and isinstance(stream, BaseStream)):
            return func(*args, **kw)

        is_consumed = getattr(stream, "_is_consumed", None)
        if is_consumed:
            raise IllegalStateError("Stream object already consumed")

        result = func(*args, **kw)
        if is_consumed is False and func.__name__ in TERMINAL_FUNCTIONS:
            stream._is_consumed = True
            # if stream._on_close_handler:
            #     stream._on_close_handler()
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
                remapped.append((DictItem(k, v) for k, v in arg.items()))
            else:
                remapped.append(arg)
        return func(*remapped, **kw)

    return wrapper

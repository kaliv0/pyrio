from collections.abc import Mapping
from functools import wraps

from pyrio.utils.dict_item import DictItem
from pyrio.utils.exception import IllegalStateError

TERMINAL_FUNCTIONS = [
    "for_each",
    "reduce",
    "count",
    "min",
    "max",
    "sum",
    "average",
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
        if is_consumed and func.__name__ != "close":
            raise IllegalStateError("Stream object already consumed")

        result = func(*args, **kw)
        if not is_consumed and func.__name__ in TERMINAL_FUNCTIONS:
            stream.close()
        return result

    return wrapper


# ### util for concat ###
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


# ### multiple dispatch ###
METHOD_REGISTRY = {}


class MultiMethod:
    def __init__(self, name):
        self.name = name
        self.typemap = {}

    def __call__(self, cls, *args):
        first_arg_type = args[0].__class__
        function = self.typemap.get(first_arg_type)
        return function(cls, *args)

    def register(self, type_, function):
        self.typemap[type_] = function


def dispatch(*types):
    def register(method):
        name = method.__name__
        multi_method = METHOD_REGISTRY.get(name)
        if multi_method is None:
            multi_method = METHOD_REGISTRY[name] = MultiMethod(name)
        for type_ in types:
            multi_method.register(type_, method)
        return multi_method

    return register

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
# inspired from https://www.artima.com/weblogs/viewpost.jsp?thread=101605

METHOD_REGISTRY = {}


class MultiMethod:
    def __init__(self, name):
        self.name = name
        self.typemap = {}

    def __call__(self, cls, *args):
        types = tuple(arg.__class__ for arg in args)
        function = self.typemap.get(types)
        if function is None:
            raise TypeError("No match found")
        return function(cls, *args)

    def register(self, types, function):
        if types in self.typemap:
            raise TypeError("Duplicate registration")
        self.typemap[types] = function


def dispatch(*types):
    def register(function):
        function = getattr(function, "__lastreg__", function)
        name = function.__name__
        multi_method = METHOD_REGISTRY.get(name)
        if multi_method is None:
            multi_method = METHOD_REGISTRY[name] = MultiMethod(name)
        multi_method.register(types, function)
        multi_method.__lastreg__ = function
        return multi_method

    return register

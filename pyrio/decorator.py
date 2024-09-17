from functools import wraps

from pyrio.exception import IllegalStateError

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
        from pyrio import Stream

        if not args or isinstance(args[0], Stream) is False:
            return func(*args, **kw)

        is_consumed = getattr(args[0], "_is_consumed", None)
        if is_consumed:
            raise IllegalStateError("Stream object already consumed")

        result = func(*args, **kw)
        if is_consumed is False and func.__name__ in TERMINAL_FUNCTIONS:
            args[0]._is_consumed = True
        return result

    return wrapper

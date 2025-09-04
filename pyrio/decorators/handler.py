from __future__ import annotations
from functools import wraps
from typing import Any, Callable, TypeVar

from pyrio.exceptions import IllegalStateError

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


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


def pre_call(function_decorator: Callable[[F], F]) -> Callable[[type[T]], type[T]]:
    def decorator(cls: type[T]) -> type[T]:
        for name, obj in vars(cls).items():
            if callable(obj):
                setattr(cls, name, function_decorator(obj))  # type: ignore
        return cls

    return decorator


def handle_consumed(func: F) -> F:
    @wraps(func)
    def wrapper(*args: Any, **kw: Any) -> Any:
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

    return wrapper  # type: ignore

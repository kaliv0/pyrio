from __future__ import annotations
from collections.abc import Mapping
from functools import wraps
from typing import Any, Callable, TypeVar

from pyrio.utils import DictItem

F = TypeVar("F", bound=Callable[..., Any])


def map_dict_items(func: F) -> F:
    @wraps(func)
    def wrapper(*args: Any, **kw: Any) -> Any:
        if not any(isinstance(arg, Mapping) for arg in args):
            return func(*args, **kw)

        remapped: list[Any] = []
        for arg in args:
            if isinstance(arg, Mapping):
                remapped.append((DictItem(k, v) for k, v in arg.items()))
            else:
                remapped.append(arg)
        return func(*remapped, **kw)

    return wrapper  # type: ignore

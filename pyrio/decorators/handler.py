from functools import wraps
from types import FunctionType

from pyrio.exceptions import IllegalStateError


def terminal(func):
    """Marks a stream method as terminal (consumes and closes the stream)."""
    func._terminal = True
    return func


def is_terminal(func):
    return getattr(func, "_terminal", False)


def pre_call(function_decorator):
    """Wrap public instance methods."""

    def decorator(cls):
        for name, obj in vars(cls).items():
            # skip privates / dunders / class / static
            if not name.startswith("_") and isinstance(obj, FunctionType):
                setattr(cls, name, function_decorator(obj))
        return cls

    return decorator


def handle_consumed(func):
    """Block ops on consumed streams, auto-close after @terminal methods."""

    @wraps(func)
    def wrapper(self, *args, **kw):
        from pyrio.streams.base_stream import BaseStream

        if not isinstance(self, BaseStream):
            return func(self, *args, **kw)  # pragma: no cover

        # __dict__ avoids __getattr__ (no recursion if flag missing mid-init)
        consumed = self.__dict__.get("_is_consumed", False)
        if consumed and func.__name__ != "close":
            raise IllegalStateError("Stream object already consumed")

        result = func(self, *args, **kw)
        if not consumed and getattr(func, "_terminal", False):
            self.close()
        return result

    wrapper._terminal = getattr(func, "_terminal", False)
    return wrapper

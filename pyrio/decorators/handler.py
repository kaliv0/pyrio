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
            # skip privates & dunders
            if name.startswith("_") or name == "close":
                continue
            # apply only on instance methods
            if isinstance(obj, FunctionType):
                setattr(cls, name, function_decorator(obj))
        return cls

    return decorator


def handle_consumed(func):
    """Block ops on consumed streams, auto-close after @terminal methods."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        from pyrio.streams.base_stream import BaseStream

        if not isinstance(self, BaseStream):
            return func(self, *args, **kwargs)  # pragma: no cover

        # __dict__ avoids __getattr__ (no recursion if flag missing mid-init)
        if self.__dict__.get("_is_consumed", False):
            raise IllegalStateError("Stream object already consumed")

        if not is_terminal(func):
            return func(self, *args, **kwargs)

        try:
            return func(self, *args, **kwargs)
        finally:
            self.close()

    wrapper._terminal = is_terminal(func)
    return wrapper

from functools import wraps

from pyrio.exception import IllegalStateError


def pre_call(function_decorator):
    def decorator(cls):
        for name, obj in vars(cls).items():
            if callable(obj):
                setattr(cls, name, function_decorator(obj))
        return cls

    return decorator


def validate_stream(func):
    @wraps(func)
    def wrapper(*args, **kw):
        from pyrio import Stream

        if args and isinstance(args[0], Stream):
            if getattr(args[0], "_is_consumed", None):
                raise IllegalStateError("Stream object already consumed")
        return func(*args, **kw)

    return wrapper

import itertools as it
from collections import deque
from collections.abc import Iterable, Sized

from pyrio.iterator import Iterator


class ItertoolsMixin:
    _iterable: Iterable | Sized

    # FIXME
    # def __init__(self):
    #     self._iterable = None

    def use(self, it_function, *args, **kwargs):
        import inspect

        if args:
            raise ValueError("Use keyword arguments only")

        if self._handle_no_signature_functions(it_function, **kwargs):
            return self

        signature = inspect.signature(it_function).parameters
        if self._handle_no_kwargs_functions(signature, it_function, **kwargs):
            return self

        return self._handle_default_signature_functions(signature, it_function, **kwargs)

    def _handle_no_signature_functions(self, it_function, **kwargs):
        NO_SIGNATURE_FUNCTIONS = ["chain", "islice", "product", "repeat", "zip_longest"]

        if it_function.__name__ not in NO_SIGNATURE_FUNCTIONS:
            return False

        if it_function.__name__ in ("product", "zip_longest"):
            if isinstance(self._iterable, range):
                self._iterable = it_function(self._iterable, **kwargs)
            else:
                self._iterable = it_function(*self._iterable, **kwargs)
            return True

        # functions like 'chain' don't expect key-word arguments
        self._iterable = it_function(self._iterable, *kwargs.values())
        return True

    def _handle_no_kwargs_functions(self, signature, it_function, **kwargs):
        NO_KWARGS_FUNCTIONS = ["dropwhile", "filterfalse", "starmap", "takewhile", "tee"]

        # handle functions that take only iterable as arg
        if len(signature.keys()) == 1 and "iterable" in signature:
            self._iterable = it_function(self._iterable)
            return True

        # handle functions that take no kwargs
        if it_function.__name__ in NO_KWARGS_FUNCTIONS:
            if it_function.__name__ == "tee":
                self._iterable = it_function(self._iterable, *kwargs.values())
            else:
                self._iterable = it_function(*kwargs.values(), self._iterable)
            return True
        return False

    def _handle_default_signature_functions(self, signature, it_function, **kwargs):
        if "iterable" in signature:
            kwargs["iterable"] = self._iterable
        elif "data" in signature:
            kwargs["data"] = self._iterable
        self._iterable = it_function(**kwargs)
        return self

    # ### 'recipes' ###
    def tabulate(self, mapper, start=0):
        self._iterable = Iterator.map(it.count(start), mapper)  # TODO: fix?
        return self

    def repeat_func(self, operation, times=None):
        self._iterable = it.starmap(operation, it.repeat(self._iterable, times=times))
        return self

    def ncycles(self, count=0):
        self._iterable = it.chain.from_iterable(it.repeat(tuple(self._iterable), count))
        return self

    def consume(self, n=None):
        if n is None:
            self._iterable = deque(self._iterable, maxlen=0)
            return self
        if n < 0:
            raise ValueError("Consume boundary cannot be negative")
        self._iterable = it.islice(self._iterable, n, len(self._iterable))
        return self

    def nth(self, idx, default=None):
        # FIXME
        """Returns empty iterable if count is zero or negative"""
        if idx < 0:
            idx = len(self._iterable) + idx
        return next(it.islice(self._iterable, idx, None), default)

    def all_equal(self, key=None):
        return len(list(it.islice(it.groupby(self._iterable, key), 2))) <= 1

    # TODO: start=None?
    def view(self, start=0, stop=None, step=None):
        if start < 0:
            start = len(self._iterable) + start

        if stop and stop < 0:
            stop = len(self._iterable) + stop

        if step and step < 0:
            raise ValueError("Step must be a positive integer or None")

        self._iterable = it.islice(self._iterable, start, stop, step)
        return self

    # def unique():
    # def unique_justseen():
    # def unique_everseen():

    def sliding_window(self, n):
        if n < 0:
            raise ValueError("Window size cannot be negative")
        self._iterable = self._sliding_window(self._iterable, n)
        return self

    @staticmethod
    def _sliding_window(iterable, n):
        """Collect data into overlapping fixed-length chunks or blocks"""
        window = deque(it.islice(iterable, n - 1), maxlen=n)
        for x in it.islice(iterable, n - 1, len(iterable)):
            window.append(x)
            yield tuple(window)

    # def grouper():
    # def round_robin():
    # def partition():
    # def subslices():
    # def iter_index(): -> rename??

import itertools as it
import operator
from functools import wraps

from pyrio.exceptions import MethodNotFoundError
from pyrio.utils import Optional


class ItertoolsMixin:
    """Provides integration with itertools methods. Pass corresponding parameters as kwargs"""

    iterable = None

    def __getattr__(self, item):
        func = getattr(it, item, None)
        if func is None:
            raise MethodNotFoundError(f"'{item}' not found")

        @wraps(func)
        def wrapper(**kwargs):
            return self._integrate(func, **kwargs)

        return wrapper

    def _integrate(self, it_function, **kwargs):
        import inspect

        try:
            signature = inspect.signature(it_function).parameters
        except ValueError:
            signature = {}

        match it_function.__name__:
            # handle functions that take no kwargs
            case "islice" | "repeat" | "tee" | "chain":
                self.iterable = it_function(self.iterable, *kwargs.values())
            case "dropwhile" | "filterfalse" | "starmap" | "takewhile" | "islice" | "repeat":
                self.iterable = it_function(*kwargs.values(), self.iterable)
            # mixed
            case "product" | "zip_longest":
                self.iterable = it_function(*self.iterable, **kwargs)
            # only iterable as arg
            case _:
                if len(signature) == 1 and "iterable" in signature:
                    self.iterable = it_function(self.iterable)
                # all kwargs
                else:
                    if sequence := next(
                        (name for name in {"iterable", "data"} if name in signature), None
                    ):
                        kwargs[sequence] = self.iterable
                    self.iterable = it_function(**kwargs)

        return self

    # ### 'recipes' ###
    # https://docs.python.org/3/library/itertools.html#itertools-recipes
    def tabulate(self, mapper, start=0):
        """Returns function(0), function(1), ..."""
        self.iterable = map(mapper, it.count(start))
        return self

    def repeat_func(self, operation, times=None):
        """Repeats calls to func with specified arguments"""
        self.iterable = it.starmap(operation, it.repeat(self.iterable, times=times))
        return self

    def ncycles(self, count=0):
        """Returns the stream elements n times"""
        self.iterable = it.chain.from_iterable(it.repeat(tuple(self.iterable), count))
        return self

    def consume(self, n=None):
        """Advances the iterator n-steps ahead. If n is None, consumes stream entirely"""
        import collections

        if n is None:
            self.iterable = collections.deque(self.iterable, maxlen=0)
            return self
        if n < 0:
            raise ValueError("Consume boundary cannot be negative")
        self.iterable = it.islice(self.iterable, n, len(self.iterable))
        return self

    def take_nth(self, idx, default=None):
        """Returns Optional with the nth element of the stream or a default value"""
        if idx < 0:
            idx = len(self.iterable) + idx
        return Optional.of_nullable(next(it.islice(self.iterable, idx, None), default))

    def all_equal(self, key=None):
        """Returns True if all elements of the stream are equal to each other"""
        return len(list(it.islice(it.groupby(self.iterable, key), 2))) <= 1

    def view(self, start=0, stop=None, step=None):
        """Provides access to a selected part of the stream"""
        if start < 0:
            start = len(self.iterable) + start

        if stop and stop < 0:
            stop = len(self.iterable) + stop

        if step and step < 0:
            raise ValueError("Step must be a positive integer or None")

        self.iterable = it.islice(self.iterable, start, stop, step)
        return self

    # ### unique ###
    def unique(self, key=None, reverse=False):
        """Yields unique elements in sorted order. Supports unhashable inputs"""
        self.iterable = self._unique(sorted(self.iterable, key=key, reverse=reverse), key=key)
        return self

    @staticmethod
    def _unique(iterable, key=None):
        return map(next, map(operator.itemgetter(1), it.groupby(iterable, key)))

    def unique_just_seen(self, key=None):
        """Yields unique elements, preserving order. Remembers only the element just seen"""
        self.iterable = map(next, map(operator.itemgetter(1), it.groupby(self.iterable, key)))
        return self

    def unique_ever_seen(self, key=None):
        """Yields unique elements, preserving order. Remembers all elements ever seen"""
        self.iterable = self._unique_ever_seen(self.iterable, key)
        return self

    @staticmethod
    def _unique_ever_seen(iterable, key=None):
        seen = set()
        for element in iterable:
            k = key(element) if key else element
            if k not in seen:
                seen.add(k)
                yield element

    # ### ###
    def sliding_window(self, n):
        """Collects data into overlapping fixed-length chunks or blocks"""
        if n < 0:
            raise ValueError("Window size cannot be negative")
        self.iterable = self._sliding_window(self.iterable, n)
        return self

    @staticmethod
    def _sliding_window(iterable, n):
        import collections

        window = collections.deque(it.islice(iterable, n - 1), maxlen=n)
        for x in it.islice(iterable, n - 1, len(iterable)):
            window.append(x)
            yield tuple(window)

    def grouper(self, n, *, incomplete="fill", fill_value=None):
        """Collects data into non-overlapping fixed-length chunks or blocks"""
        self.iterable = self._grouper(n, incomplete, fill_value)
        return self

    def _grouper(self, n, incomplete="fill", fill_value=None):
        iterators = [iter(self.iterable)] * n
        match incomplete:
            case "fill":
                return it.zip_longest(*iterators, fillvalue=fill_value)
            case "strict":
                return zip(*iterators, strict=True)
            case "ignore":
                return zip(*iterators)
            case _:
                raise ValueError(
                    f"Invalid incomplete flag '{incomplete}', expected: 'fill', 'strict', or 'ignore'"
                )

    def round_robin(self):
        """Visits input iterables in a cycle until each is exhausted"""
        self.iterable = self._round_robin(self.iterable)
        return self

    @staticmethod
    def _round_robin(iterable):
        # Algorithm credited to George Sakkis
        iterators = map(iter, iterable)
        for num_active in range(len(iterable), 0, -1):
            iterators = it.cycle(it.islice(iterators, num_active))
            yield from map(next, iterators)

    def partition(self, predicate):
        """
        Partitions entries into true and false entries.
        Returns a stream of two nested generators
        """
        true_iter, false_iter = it.tee(self.iterable)
        self.iterable = filter(predicate, true_iter), it.filterfalse(predicate, false_iter)
        return self

    def subslices(self):
        """Returns all contiguous non-empty sub-slices"""
        slices = it.starmap(slice, it.combinations(range(len(self.iterable) + 1), 2))
        self.iterable = map(operator.getitem, it.repeat(self.iterable), slices)  # noqa
        return self

    def find_indices(self, value, start=0, stop=None):
        """Returns indices where a value occurs in a sequence or iterable"""
        self.iterable = self._find_indices(self.iterable, value, start, stop)
        return self

    @staticmethod
    def _find_indices(iterable, value, start=0, stop=None):
        iterator = it.islice(iterable, start, stop)
        for i, element in enumerate(iterator, start):
            if element is value or element == value:
                yield i

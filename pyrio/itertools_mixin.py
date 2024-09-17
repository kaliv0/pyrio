from collections.abc import Iterable, Sized
import itertools as it
import operator

from pyrio.optional import Optional


class ItertoolsMixin:
    _iterable: Iterable | Sized

    def use(self, it_function, *args, **kwargs):
        """Provides integration with itertools methods"""
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
    # https://docs.python.org/3/library/itertools.html#itertools-recipes
    def tabulate(self, mapper, start=0):
        """ "Returns function(0), function(1), ..."""
        self._iterable = map(mapper, it.count(start))
        return self

    def repeat_func(self, operation, times=None):
        """Repeats calls to func with specified arguments"""
        self._iterable = it.starmap(operation, it.repeat(self._iterable, times=times))
        return self

    def ncycles(self, count=0):
        """Returns the stream elements n times"""
        self._iterable = it.chain.from_iterable(it.repeat(tuple(self._iterable), count))
        return self

    def consume(self, n=None):
        """Advances the iterator n-steps ahead. If n is None, consumes stream entirely"""
        import collections

        if n is None:
            self._iterable = collections.deque(self._iterable, maxlen=0)
            return self
        if n < 0:
            raise ValueError("Consume boundary cannot be negative")
        self._iterable = it.islice(self._iterable, n, len(self._iterable))
        return self

    def take_nth(self, idx, default=None):
        """Returns Optional with the nth element of the stream or a default value"""
        if idx < 0:
            idx = len(self._iterable) + idx
        return Optional.of_nullable(next(it.islice(self._iterable, idx, None), default))

    def all_equal(self, key=None):
        """Returns True if all elements of the stream are equal to each other"""
        return len(list(it.islice(it.groupby(self._iterable, key), 2))) <= 1

    def view(self, start=0, stop=None, step=None):
        """Provides access to a selected part of the stream"""
        if start < 0:
            start = len(self._iterable) + start

        if stop and stop < 0:
            stop = len(self._iterable) + stop

        if step and step < 0:
            raise ValueError("Step must be a positive integer or None")

        self._iterable = it.islice(self._iterable, start, stop, step)
        return self

    # ### unique ###
    def unique(self, key=None, reverse=False):
        """Yields unique elements in sorted order. Supports unhashable inputs"""
        self._iterable = self._unique(sorted(self._iterable, key=key, reverse=reverse), key=key)
        return self

    @staticmethod
    def _unique(iterable, key=None):
        return map(next, map(operator.itemgetter(1), it.groupby(iterable, key)))

    def unique_just_seen(self, key=None):
        """Yields unique elements, preserving order. Remembers only the element just seen"""
        self._iterable = map(next, map(operator.itemgetter(1), it.groupby(self._iterable, key)))
        return self

    def unique_ever_seen(self, key=None):
        """Yields unique elements, preserving order. Remembers all elements ever seen"""
        self._iterable = self._unique_ever_seen(self._iterable, key)
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
        self._iterable = self._sliding_window(self._iterable, n)
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
        self._iterable = self._grouper(n, incomplete, fill_value)
        return self

    def _grouper(self, n, incomplete="fill", fill_value=None):
        iterators = [iter(self._iterable)] * n
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
        self._iterable = self._round_robin(self._iterable)
        return self

    @staticmethod
    def _round_robin(iterable):
        # Algorithm credited to George Sakkis
        iterators = map(iter, iterable)
        for num_active in range(len(iterable), 0, -1):
            iterators = it.cycle(it.islice(iterators, num_active))
            yield from map(next, iterators)

    def partition(self, predicate):
        """Partitions entries into false entries and true entries.
        Returns a stream of two nested generators"""
        true_iter, false_iter = it.tee(self._iterable)
        self._iterable = filter(predicate, true_iter), it.filterfalse(predicate, false_iter)
        return self

    def subslices(self):
        """Returns all contiguous non-empty sub-slices"""
        slices = it.starmap(slice, it.combinations(range(len(self._iterable) + 1), 2))
        self._iterable = map(operator.getitem, it.repeat(self._iterable), slices)  # noqa
        return self

    def find_indices(self, value, start=0, stop=None):
        """Returns indices where a value occurs in a sequence or iterable"""
        self._iterable = self._find_indices(self._iterable, value, start, stop)
        return self

    @staticmethod
    def _find_indices(iterable, value, start=0, stop=None):
        iterator = it.islice(iterable, start, stop)
        for i, element in enumerate(iterator, start):
            if element is value or element == value:
                yield i

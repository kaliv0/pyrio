from collections.abc import Iterable

from pyrio.decorators import map_dict_items


class StreamGenerator:
    """Helper class wrapping generator-based operations for lazy evaluation"""

    @staticmethod
    @map_dict_items
    def concat(*streams):
        """Concatenates multiple iterables into a single sequence"""
        for iterable in streams:
            yield from iterable

    @staticmethod
    def filter(iterable, predicate):
        """Yields elements that satisfy the predicate"""
        for i in iterable:
            if predicate(i):
                yield i

    @staticmethod
    def map(iterable, mapper):
        """Applies mapper function to each element"""
        for i in iterable:
            yield mapper(i)

    @staticmethod
    def filter_map(iterable, mapper, discard_falsy=False):
        """Filters out None (or falsy) values and applies mapper to remaining elements"""
        for i in iterable:
            if (not discard_falsy and i is not None) or (discard_falsy and i):
                yield mapper(i)

    @staticmethod
    def flat_map(iterable, mapper):
        """Applies mapper and flattens the resulting iterables"""
        for i in iterable:
            yield from mapper(i)

    @classmethod
    def flatten(cls, iterable):
        """Recursively flattens nested iterables into a single sequence"""
        for i in iterable:
            if isinstance(i, str) or not isinstance(i, Iterable):
                yield i
            else:
                yield from cls.flatten(i)

    @staticmethod
    def peek(iterable, operation):
        """Performs operation on each element without consuming the stream"""
        for i in iterable:
            operation(i)
            yield i

    @staticmethod
    def iterate(seed, operation, condition=None):
        """Generates sequence by repeatedly applying operation to seed"""
        if condition is None:
            condition = lambda _: True  # noqa
        while condition(seed):
            yield seed
            seed = operation(seed)

    @staticmethod
    def generate(supplier):
        """Generates infinite sequence using supplier function"""
        while True:
            yield supplier()

    @staticmethod
    def range(start, stop, step=1):
        """Yields values from start to stop with given step"""
        for i in range(start, stop, step):
            yield i

    @staticmethod
    def distinct(iterable):
        """Yields unique elements preserving first occurrence order"""
        elements = set()
        for i in iterable:
            if i not in elements:
                elements.add(i)
                yield i

    @staticmethod
    def skip(iterable, count):
        """Skips first n elements and yields the rest"""
        for i in iterable:
            if count > 0:
                count -= 1
            else:
                yield i

    @staticmethod
    def limit(iterable, count):
        """Yields at most n elements"""
        for i in iterable:
            if count == 0:
                break
            yield i
            count -= 1

    @staticmethod
    def tail(iterable, count):
        """Yields the last n elements"""
        import collections

        for i in collections.deque(iterable, maxlen=count):
            yield i

    @staticmethod
    def take_while(iterable, predicate):
        """Yields elements while predicate is true"""
        for i in iterable:
            if not predicate(i):
                break
            yield i

    @staticmethod
    def drop_while(iterable, predicate):
        """Skips elements while predicate is true, then yields the rest"""
        iterator = iter(iterable)
        for x in iterator:
            if not predicate(x):
                yield x
                break

        for x in iterator:
            yield x

    @staticmethod
    def sort(iterable, comparator=None, reverse=False):
        """Yields elements in sorted order"""
        for i in sorted(iterable, key=comparator, reverse=reverse):
            yield i

    @staticmethod
    def enumerate(iterable, start=0):
        """Yields index-element pairs starting from given index"""
        for i, item in enumerate(iterable, start):
            yield i, item

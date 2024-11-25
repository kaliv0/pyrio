from collections.abc import Iterable

from pyrio.utils.decorator import map_dict_items


class Generator:
    @staticmethod
    @map_dict_items
    def concat(*streams):
        for iterable in streams:
            yield from iterable

    @staticmethod
    def filter(iterable, predicate):
        for i in iterable:
            if predicate(i):
                yield i

    @staticmethod
    def map(iterable, mapper):
        for i in iterable:
            yield mapper(i)

    @staticmethod
    def filter_map(iterable, mapper, falsy=False):
        for i in iterable:
            if (not falsy and i is not None) or (falsy and i):
                yield mapper(i)

    @staticmethod
    def flat_map(iterable, mapper):
        for i in iterable:
            yield from mapper(i)

    @classmethod
    def flatten(cls, iterable):
        for i in iterable:
            if isinstance(i, str) or not isinstance(i, Iterable):
                yield i
            else:
                yield from cls.flatten(i)

    @staticmethod
    def peek(iterable, operation):
        for i in iterable:
            operation(i)
            yield i

    @staticmethod
    def iterate(seed, operation):
        while True:
            yield seed
            seed = operation(seed)

    @staticmethod
    def generate(supplier):
        while True:
            yield supplier()

    @staticmethod
    def distinct(iterable):
        elements = set()
        for i in iterable:
            if i not in elements:
                elements.add(i)
                yield i

    @staticmethod
    def skip(iterable, count):
        for i in iterable:
            if count > 0:
                count -= 1
            else:
                yield i

    @staticmethod
    def limit(iterable, count):
        for i in iterable:
            if count == 0:
                break
            yield i
            count -= 1

    @staticmethod
    def tail(iterable, count):
        import collections

        for i in collections.deque(iterable, maxlen=count):
            yield i

    @staticmethod
    def take_while(iterable, predicate):
        for i in iterable:
            if not predicate(i):
                break
            yield i

    @staticmethod
    def drop_while(iterable, predicate):
        iterator = iter(iterable)
        for x in iterator:
            if not predicate(x):
                yield x
                break

        for x in iterator:
            yield x

    @staticmethod
    def sorted(iterable, comparator=None, reverse=False):
        for i in sorted(iterable, key=comparator, reverse=reverse):
            yield i

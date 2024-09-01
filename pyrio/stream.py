import random

from pyrio.decorator import pre_call, validate_stream
from pyrio.iterator import Iterator
from pyrio.optional import Optional


@pre_call(validate_stream)
class Stream:
    def __init__(self, iterable):
        """creates Stream from a collection"""
        self._iterable = iterable
        self._is_consumed = False

    def __iter__(self):
        return iter(self._iterable)

    @staticmethod
    def of(*iterable):
        """creates Stream from args"""
        return Stream(iterable)

    @staticmethod
    def empty():
        return Stream([])

    @staticmethod
    def iterate(seed, operation):
        """creates infinite Stream"""
        return Stream(Iterator.iterate(seed, operation))

    @staticmethod
    def generate(supplier):
        """creates infinite Stream"""
        return Stream(Iterator.generate(supplier))

    @staticmethod
    def concat(*streams):
        return Stream(Iterator.concat(*streams))

    def filter(self, predicate):
        self._iterable = Iterator.filter(self._iterable, predicate)
        return self

    def map(self, mapper):
        self._iterable = Iterator.map(self._iterable, mapper)
        return self

    def filter_map(self, predicate, *, falsy=False):
        self._iterable = Iterator.filter_map(self._iterable, predicate, falsy)
        return self

    def flat_map(self, mapper):
        self._iterable = Iterator.flat_map(self._iterable, mapper)
        return self

    def flatten(self):
        self._iterable = Iterator.flatten(self._iterable)
        return self

    def for_each(self, operation):
        self._is_consumed = True
        return Iterator.for_each(self._iterable, operation)

    def peek(self, operation):
        self._iterable = Iterator.peek(self._iterable, operation)
        return self

    def reduce(self, accumulator, identity=None):
        self._is_consumed = True
        return Optional.of_nullable(Iterator.reduce(self._iterable, accumulator, identity))

    def distinct(self):
        self._iterable = Iterator.distinct(self._iterable)
        return self

    def count(self):
        self._is_consumed = True
        return len(tuple(self._iterable))

    def sum(self):
        self._is_consumed = True
        if len(self._iterable) == 0:
            return 0
        if any(isinstance(x, (int | float | None)) for x in self._iterable) is False:
            raise ValueError("Cannot apply sum on non-number elements")
        return sum(self._iterable)

    def skip(self, count):
        if count < 0:
            raise ValueError("Skip count cannot be negative")
        self._iterable = Iterator.skip(self._iterable, count)
        return self

    def limit(self, count):
        if count < 0:
            raise ValueError("Limit count cannot be negative")
        self._iterable = Iterator.limit(self._iterable, count)
        return self

    def take_while(self, predicate):
        self._iterable = Iterator.take_while(self._iterable, predicate)
        return self

    def drop_while(self, predicate):
        self._iterable = Iterator.drop_while(self._iterable, predicate)
        return self

    def find_first(self, predicate=None):
        if predicate:
            self.filter(predicate)

        self._is_consumed = True
        for i in self._iterable:
            return Optional.of(i)
        return Optional.of_nullable(None)

    def find_any(self, predicate=None):
        if predicate:
            self.filter(predicate)

        self._is_consumed = True
        try:
            return Optional.of(random.choice(list(self._iterable)))
        except IndexError:
            return Optional.of_nullable(None)

    def any_match(self, predicate):
        self._is_consumed = True
        return any(predicate(i) for i in self._iterable)

    def all_match(self, predicate):
        self._is_consumed = True
        return all(predicate(i) for i in self._iterable)

    def none_match(self, predicate):
        self._is_consumed = True
        return not all(predicate(i) for i in self._iterable)

    def min(self, comparator=None, default=None):
        self._is_consumed = True
        result = min(self._iterable, key=comparator, default=default)
        if result:
            return Optional.of(result)
        return Optional.of_nullable(None)

    def max(self, comparator=None, default=None):
        self._is_consumed = True
        result = max(self._iterable, key=comparator, default=default)
        if result:
            return Optional.of(result)
        return Optional.of_nullable(None)

    def compare_with(self, other, comparator=None):
        self._is_consumed = True
        return Iterator.compare_with(self._iterable, other, comparator)

    def sorted(self, comparator=None, *, reverse=False):
        self._iterable = Iterator.sorted(self._iterable, comparator, reverse)
        return self

    # ### collectors ###
    def collect(self, collection_type, dict_supplier=None):
        import builtins

        match collection_type:
            case builtins.tuple:
                return self.to_tuple()
            case builtins.list:
                return self.to_list()
            case builtins.set:
                return self.to_set()
            case builtins.dict:
                if dict_supplier is None:
                    raise ValueError("Missing dict_supplier")
                return self.to_dict(dict_supplier)
            case _:
                raise ValueError("Invalid collection type")

    def to_list(self):
        self._is_consumed = True
        return list(self._iterable)

    def to_tuple(self):
        self._is_consumed = True
        return tuple(self._iterable)

    def to_set(self):
        self._is_consumed = True
        return set(self._iterable)

    def to_dict(self, operation):
        self._is_consumed = True
        return {k: v for k, v in (operation(i) for i in self._iterable)}

import builtins

from pyrio.decorator import pre_call, validate_stream
from pyrio.iterator import Iterator


# ### ### #
@pre_call(validate_stream)
class Stream:
    def __init__(self, iterable):
        """creates Stream from a collection"""
        self._iterable = iterable
        self._is_consumed = False

    def __iter__(self):
        return iter(self._iterable)

    def __eq__(self, other):
        # FIXME: don't use sets, (1, 2, 3) and (1, 1, 2, 3) are not equal
        return self.to_set() == other.to_set()

    @staticmethod
    def of(*iterable):
        """creates Stream from args"""
        return Stream(iterable)

    @staticmethod
    def empty():
        return Stream([])

    # TODO: rename seed to identity?
    @staticmethod
    def iterate(seed, function):
        """creates infinite Stream"""
        return Stream(Iterator.iterate(seed, function))

    @staticmethod
    def generate(supplier):
        """creates infinite Stream"""
        return Stream(Iterator.generate(supplier))

    def filter(self, predicate):
        self._iterable = Iterator.filter(self._iterable, predicate)
        return self

    def map(self, mapper):
        self._iterable = Iterator.map(self._iterable, mapper)
        return self

    def filter_map(self, predicate):
        self._iterable = Iterator.filter_map(self._iterable, predicate)
        return self

    def flat_map(self, mapper):
        self._iterable = Iterator.flat_map(self._iterable, mapper)
        return self

    def flatten(self):
        self._iterable = Iterator.flatten(self._iterable)
        return self

    def for_each(self, function):
        self._is_consumed = True
        return Iterator.for_each(self._iterable, function)

    def reduce(self, accumulator, identity=None):
        self._is_consumed = True
        # TODO: put identity as first arg- no default value?
        return Iterator.reduce(self._iterable, accumulator, identity)

    def distinct(self):
        self._iterable = Iterator.distinct(self._iterable)
        return self

    def count(self):
        self._is_consumed = True
        return len(tuple(self._iterable))

    def sum(self):
        self._is_consumed = True
        # TODO: move logic to Iterator?
        if len(self._iterable) == 0:
            return 0
        if any(isinstance(x, (int | float | None)) for x in self._iterable) is False:
            raise ValueError("Cannot apply sum on non-number elements")
        return sum(self._iterable)

    def skip(self, count):
        # TODO: check here or in iterator?
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

    # TODO: NB force user to explicitly write reverse as kwarg
    # rename key to comparator?
    def sorted(self, key=None, *, reverse=False):
        self._iterable = Iterator.sorted(self._iterable, key, reverse)
        return self

    # ### collectors ###
    def collect(self, collection_type, dict_supplier=None):
        # TODO: refactor without builtins
        match collection_type:
            case builtins.tuple:
                return self.to_tuple()
            case builtins.list:
                return self.to_list()
            case builtins.set:
                return self.to_set()
            case builtins.dict:
                if dict_supplier is None:
                    # TODO: tests for exceptions, change messages
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

    # TODO: should this be inside Iterator?
    def to_dict(self, function):
        self._is_consumed = True
        return Iterator.to_dict(self._iterable, function)

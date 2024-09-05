from pyrio.decorator import pre_call, handle_consumed
from pyrio.exception import IllegalStateError
from pyrio.iterator import Iterator
from pyrio.itertools_mixin import ItertoolsMixin
from pyrio.optional import Optional


@pre_call(handle_consumed)
class Stream(ItertoolsMixin):
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

    def prepend(self, *iterable):
        self._iterable = Iterator.concat(iterable, self._iterable)
        return self

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
        return Iterator.for_each(self._iterable, operation)

    def peek(self, operation):
        self._iterable = Iterator.peek(self._iterable, operation)
        return self

    def reduce(self, accumulator, identity=None):
        return Optional.of_nullable(Iterator.reduce(self._iterable, accumulator, identity))

    def distinct(self):
        self._iterable = Iterator.distinct(self._iterable)
        return self

    def count(self):
        return len(tuple(self._iterable))

    def sum(self):
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

    def tail(self, count):
        if count < 0:
            raise ValueError("Take count cannot be negative")
        self._iterable = Iterator.tail(self._iterable, count)
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
        for i in self._iterable:
            return Optional.of(i)
        return Optional.of_nullable(None)

    def find_any(self, predicate=None):
        import random

        if predicate:
            self.filter(predicate)
        try:
            return Optional.of(random.choice(list(self._iterable)))
        except IndexError:
            return Optional.of_nullable(None)

    def any_match(self, predicate):
        return any(predicate(i) for i in self._iterable)

    def all_match(self, predicate):
        return all(predicate(i) for i in self._iterable)

    def none_match(self, predicate):
        return not all(predicate(i) for i in self._iterable)

    def min(self, comparator=None, default=None):
        result = min(self._iterable, key=comparator, default=default)
        if result:
            return Optional.of(result)
        return Optional.of_nullable(None)

    def max(self, comparator=None, default=None):
        result = max(self._iterable, key=comparator, default=default)
        if result:
            return Optional.of(result)
        return Optional.of_nullable(None)

    def compare_with(self, other, comparator=None):
        return Iterator.compare_with(self._iterable, other, comparator)

    def sorted(self, comparator=None, *, reverse=False):
        self._iterable = Iterator.sorted(self._iterable, comparator, reverse)
        return self

    # ### collectors ###
    def collect(self, collection_type, dict_collector=None, dict_merger=None):
        import builtins

        match collection_type:
            case builtins.tuple:
                return self.to_tuple()
            case builtins.list:
                return self.to_list()
            case builtins.set:
                return self.to_set()
            case builtins.dict:
                if dict_collector is None:
                    raise ValueError("Missing dict_collector")
                return self.to_dict(dict_collector, dict_merger)
            case _:
                raise ValueError("Invalid collection type")

    def to_list(self):
        return list(self._iterable)

    def to_tuple(self):
        return tuple(self._iterable)

    def to_set(self):
        return set(self._iterable)

    def to_dict(self, collector, merger=None):
        result = {}
        for k, v in (collector(i) for i in self._iterable):
            if k in result:
                if merger is None:
                    raise IllegalStateError(f"Key '{k}' already exists")
                v = merger(result[k], v)
            result[k] = v
        return result

    def group_by(self, classifier=None, collector=None):
        if collector is None:
            return {key: list(group) for key, group in self._group_by(classifier)}

        result = {}
        for key, group in self._group_by(classifier):
            key, group = collector(key, list(group))
            if hasattr(group, "__iter__"):
                if key not in result:
                    result[key] = []
                result[key] += group
            else:
                result[key] = group
        return result

    def _group_by(self, classifier=None):
        classifier = (lambda x: x) if classifier is None else classifier
        iterator = iter(self._iterable)
        exhausted = False

        def _grouper(target_key):
            nonlocal curr_value, curr_key, exhausted
            yield curr_value
            for curr_value in iterator:
                curr_key = classifier(curr_value)
                if curr_key != target_key:
                    return
                yield curr_value
            exhausted = True

        try:
            curr_value = next(iterator)
        except StopIteration:
            return
        curr_key = classifier(curr_value)

        while not exhausted:
            target_key = curr_key
            curr_group = _grouper(target_key)
            yield curr_key, curr_group
            if curr_key == target_key:
                for _ in curr_group:
                    pass

    # ### 'recipes' ###
    def tabulate(self, mapper, start=0):
        self._iterable = ItertoolsMixin.tabulate(mapper, start)
        return self

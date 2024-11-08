from abc import ABC, abstractmethod

from pyrio.decorator import pre_call, handle_consumed
from pyrio.exception import IllegalStateError
from pyrio.iterator import Iterator
from pyrio.optional import Optional


@pre_call(handle_consumed)
class BaseStream(ABC):
    @abstractmethod
    def __init__(self, iterable):
        self._iterable = iterable
        self._is_consumed = False

    def __iter__(self):
        return iter(self._iterable)

    def filter(self, predicate):
        """Filters values in stream based on given predicate function"""
        self._iterable = Iterator.filter(self._iterable, predicate)
        return self

    def map(self, mapper):
        """Returns a stream consisting of the results of applying the given function to the elements of this stream"""
        self._iterable = Iterator.map(self._iterable, mapper)
        return self

    def filter_map(self, mapper, *, falsy=False):
        """Filters out all None or falsy values and applies mapper function to the elements of the stream"""
        self._iterable = Iterator.filter_map(self._iterable, mapper, falsy)
        return self

    def flat_map(self, mapper):
        """Maps each element of the stream and yields the elements of the produced iterators"""
        self._iterable = Iterator.flat_map(self._iterable, mapper)
        return self

    def flatten(self):
        """Converts a Stream of multidimensional collection into a one-dimensional"""
        self._iterable = Iterator.flatten(self._iterable)
        return self

    def for_each(self, operation):
        """Performs an action for each element of this stream"""
        return Iterator.for_each(self._iterable, operation)

    def peek(self, operation):
        """Performs the provided operation on each element of the stream without consuming it"""
        self._iterable = Iterator.peek(self._iterable, operation)
        return self

    def reduce(self, accumulator, identity=None):
        """Reduces the elements to a single one, by repeatedly applying a reducing operation.
        Returns Optional with the result, if any, or None"""
        return Optional.of_nullable(Iterator.reduce(self._iterable, accumulator, identity))

    def distinct(self):
        """Returns a stream with the distinct elements of the current one"""
        self._iterable = Iterator.distinct(self._iterable)
        return self

    def count(self):
        """Returns the count of elements in the stream"""
        return len(tuple(self._iterable))

    def sum(self):
        """Sums the elements of the stream"""
        if len(self._iterable) == 0:
            return 0
        if not any(isinstance(x, (int | float | None)) for x in self._iterable):
            raise ValueError("Cannot apply sum on non-number elements")
        return sum(self._iterable)

    def skip(self, count):
        """Discards the first n elements of the stream and returns a new stream with the remaining ones"""
        if count < 0:
            raise ValueError("Skip count cannot be negative")
        self._iterable = Iterator.skip(self._iterable, count)
        return self

    def limit(self, count):
        """Returns a stream with the first n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Limit count cannot be negative")
        self._iterable = Iterator.limit(self._iterable, count)
        return self

    def head(self, count):
        """Alias for 'limit'"""
        if count < 0:
            raise ValueError("Head count cannot be negative")
        self._iterable = Iterator.limit(self._iterable, count)
        return self

    def tail(self, count):
        """Returns a stream with the last n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Tail count cannot be negative")
        self._iterable = Iterator.tail(self._iterable, count)
        return self

    def take_while(self, predicate):
        """Returns a stream that yields elements based on a predicate"""
        self._iterable = Iterator.take_while(self._iterable, predicate)
        return self

    def drop_while(self, predicate):
        """Returns a stream that skips elements based on a predicate and yields the remaining ones"""
        self._iterable = Iterator.drop_while(self._iterable, predicate)
        return self

    def find_first(self, predicate=None):
        """Searches for an element of the stream that satisfies a predicate.
        Returns an Optional with the first found value, if any, or None"""
        return Optional.of_nullable(next(filter(predicate, self._iterable), None))

    def find_any(self, predicate=None):
        """Searches for an element of the stream that satisfies a predicate.
        Returns an Optional with some of the found values, if any, or None"""
        import random

        if predicate:
            self.filter(predicate)
        try:
            return Optional.of(random.choice(list(self._iterable)))
        except IndexError:
            return Optional.of_nullable(None)

    def any_match(self, predicate):
        """Returns whether any elements of the stream match the given predicate"""
        return any(predicate(i) for i in self._iterable)

    def all_match(self, predicate):
        """Returns whether all elements of the stream match the given predicate"""
        return all(predicate(i) for i in self._iterable)

    def none_match(self, predicate):
        """Returns whether no elements of the stream match the given predicate"""
        return any(not predicate(i) for i in self._iterable)

    def min(self, comparator=None, default=None):
        """Returns the minimum element of the stream according to the given comparator"""
        return Optional.of_nullable(min(self._iterable, key=comparator, default=default))

    def max(self, comparator=None, default=None):
        """Returns the maximum element of the stream according to the given comparator"""
        return Optional.of_nullable(max(self._iterable, key=comparator, default=default))

    def compare_with(self, other, comparator=None):
        """Compares current stream with another one based on a given comparator"""
        return Iterator.compare_with(self._iterable, other, comparator)

    def sorted(self, comparator=None, *, reverse=False):
        """Sorts the elements of the current stream according to natural order or based on the given comparator.
        If 'reverse' flag is True, the elements are sorted in descending order"""
        self._iterable = Iterator.sorted(self._iterable, comparator, reverse)
        return self

    # ### collectors ###
    def collect(self, collection_type, dict_collector=None, dict_merger=None):
        """Returns a collections from the stream.

        In case of dict:
        The 'dict_collector' function receives an element from the stream and returns a (key, value) pair
        specifying how the dict should be constructed.

        The 'dict_merger' functions indicates in the case of a collision (duplicate keys), which entry should be kept.
        E.g. lambda old, new: new"""
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
        """Returns a list of the elements of the current stream"""
        return list(self._iterable)

    def to_tuple(self):
        """Returns a tuple of the elements of the current stream"""
        return tuple(self._iterable)

    def to_set(self):
        """Returns a set of the elements of the current stream"""
        return set(self._iterable)

    def to_dict(self, collector, merger=None):
        """Returns a dict of the elements of the current stream.

        The 'collector' function receives an element from the stream and returns a (key, value) pair
        specifying how the dict should be constructed.

        The 'merger' functions indicates in the case of a collision (duplicate keys), which entry should be kept.
        E.g. lambda old, new: new"""
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
        # https://docs.python.org/3/library/itertools.html#itertools.groupby
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

    def quantify(self, predicate=bool):
        """Count how many of the elements are Truthy or evaluate to True based on a given predicate"""
        return sum(self.map(predicate))

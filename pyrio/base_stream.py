from typing import Mapping

from pyrio.decorator import pre_call, handle_consumed
from pyrio.exception import IllegalStateError
from pyrio.iterator import Iterator
from pyrio.optional import Optional
from pyrio.utils import Item


@pre_call(handle_consumed)
class BaseStream:
    def __init__(self, iterable):
        self._iterable = iterable
        self._is_consumed = False

    def __iter__(self):
        return iter(self.iterable)

    @property
    def iterable(self):
        if isinstance(self._iterable, Mapping):
            return (Item(key=k, value=v) for k, v in self._iterable.items())
        return self._iterable

    @iterable.setter
    def iterable(self, value):
        self._iterable = value

    @classmethod
    def empty(cls):
        """Creates empty Stream"""
        return cls([])

    @staticmethod
    def concat(*streams):
        """Concatenates several streams together or adds new streams/collections to the current one"""
        return BaseStream(Iterator.concat(*streams))

    @staticmethod
    def concat_dict(*streams):
        """Concatenates several streams from Mapping-type collections together or adds new streams/collections to the current one"""
        return BaseStream(Iterator.concat_dict(*streams))

    def prepend(self, iterable):
        """Prepends iterable to current stream"""
        self.iterable = Iterator.concat(iterable, self.iterable)
        return self

    def prepend_dict(self, iterable):
        """Prepends Mapping-type collection to current stream"""
        self.iterable = Iterator.concat_dict(iterable, self.iterable)
        return self

    def filter(self, predicate):
        """Filters values in stream based on given predicate function"""
        self.iterable = Iterator.filter(self.iterable, predicate)
        return self

    def map(self, mapper):
        """Returns a stream consisting of the results of applying the given function to the elements of this stream"""
        self.iterable = Iterator.map(self.iterable, mapper)
        return self

    def filter_map(self, mapper, *, falsy=False):
        """Filters out all None or falsy values and applies mapper function to the elements of the stream"""
        self.iterable = Iterator.filter_map(self.iterable, mapper, falsy)
        return self

    def flat_map(self, mapper):
        """Maps each element of the stream and yields the elements of the produced iterators"""
        self.iterable = Iterator.flat_map(self.iterable, mapper)
        return self

    def flatten(self):
        """Converts a Stream of multidimensional collection into a one-dimensional"""
        self.iterable = Iterator.flatten(self.iterable)
        return self

    def for_each(self, operation):
        """Performs an action for each element of this stream"""
        return Iterator.for_each(self.iterable, operation)

    def peek(self, operation):
        """Performs the provided operation on each element of the stream without consuming it"""
        self.iterable = Iterator.peek(self.iterable, operation)
        return self

    def reduce(self, accumulator, identity=None):
        """Reduces the elements to a single one, by repeatedly applying a reducing operation.
        Returns Optional with the result, if any, or None"""
        return Optional.of_nullable(Iterator.reduce(self.iterable, accumulator, identity))

    def distinct(self):
        """Returns a stream with the distinct elements of the current one"""
        self.iterable = Iterator.distinct(self.iterable)
        return self

    def count(self):
        """Returns the count of elements in the stream"""
        return len(tuple(self.iterable))

    def sum(self):
        """Sums the elements of the stream"""
        if len(self.iterable) == 0:
            return 0
        if not any(isinstance(x, (int | float | None)) for x in self.iterable):
            raise ValueError("Cannot apply sum on non-number elements")
        return sum(self.iterable)

    def skip(self, count):
        """Discards the first n elements of the stream and returns a new stream with the remaining ones"""
        if count < 0:
            raise ValueError("Skip count cannot be negative")
        self.iterable = Iterator.skip(self.iterable, count)
        return self

    def limit(self, count):
        """Returns a stream with the first n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Limit count cannot be negative")
        self.iterable = Iterator.limit(self.iterable, count)
        return self

    def head(self, count):
        """Alias for 'limit'"""
        if count < 0:
            raise ValueError("Head count cannot be negative")
        self.iterable = Iterator.limit(self.iterable, count)
        return self

    def tail(self, count):
        """Returns a stream with the last n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Tail count cannot be negative")
        self.iterable = Iterator.tail(self.iterable, count)
        return self

    def take_while(self, predicate):
        """Returns a stream that yields elements based on a predicate"""
        self.iterable = Iterator.take_while(self.iterable, predicate)
        return self

    def drop_while(self, predicate):
        """Returns a stream that skips elements based on a predicate and yields the remaining ones"""
        self.iterable = Iterator.drop_while(self.iterable, predicate)
        return self

    def find_first(self, predicate=None):
        """Searches for an element of the stream that satisfies a predicate.
        Returns an Optional with the first found value, if any, or None"""
        return Optional.of_nullable(next(filter(predicate, self.iterable), None))

    def find_any(self, predicate=None):
        """Searches for an element of the stream that satisfies a predicate.
        Returns an Optional with some of the found values, if any, or None"""
        import random

        if predicate:
            self.filter(predicate)
        try:
            return Optional.of(random.choice(list(self.iterable)))
        except IndexError:
            return Optional.of_nullable(None)

    def any_match(self, predicate):
        """Returns whether any elements of the stream match the given predicate"""
        return any(predicate(i) for i in self.iterable)

    def all_match(self, predicate):
        """Returns whether all elements of the stream match the given predicate"""
        return all(predicate(i) for i in self.iterable)

    def none_match(self, predicate):
        """Returns whether no elements of the stream match the given predicate"""
        return any(not predicate(i) for i in self.iterable)

    def min(self, comparator=None, default=None):
        """Returns the minimum element of the stream according to the given comparator"""
        return Optional.of_nullable(min(self.iterable, key=comparator, default=default))

    def max(self, comparator=None, default=None):
        """Returns the maximum element of the stream according to the given comparator"""
        return Optional.of_nullable(max(self.iterable, key=comparator, default=default))

    def compare_with(self, other, comparator=None):
        """Compares current stream with another one based on a given comparator"""
        return Iterator.compare_with(self.iterable, other, comparator)

    def sorted(self, comparator=None, *, reverse=False):
        """Sorts the elements of the current stream according to natural order or based on the given comparator.
        If 'reverse' flag is True, the elements are sorted in descending order"""
        self.iterable = Iterator.sorted(self.iterable, comparator, reverse)
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
        return list(self.iterable)

    def to_tuple(self):
        """Returns a tuple of the elements of the current stream"""
        return tuple(self.iterable)

    def to_set(self):
        """Returns a set of the elements of the current stream"""
        return set(self.iterable)

    def to_dict(self, collector, merger=None):
        """Returns a dict of the elements of the current stream.

        The 'collector' function receives an element from the stream and returns a (key, value) pair
        specifying how the dict should be constructed.

        The 'merger' functions indicates in the case of a collision (duplicate keys), which entry should be kept.
        E.g. lambda old, new: new"""
        result = {}
        for k, v in (collector(i) for i in self.iterable):
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
        iterator = iter(self.iterable)
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

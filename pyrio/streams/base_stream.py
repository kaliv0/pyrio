from collections.abc import Mapping

from pyrio.decorators import handle_consumed, pre_call, terminal
from pyrio.exceptions import IllegalStateError, NoneTypeError, UnsupportedTypeError
from pyrio.iterators import StreamGenerator
from pyrio.utils import DictItem, Optional


@pre_call(handle_consumed)
class BaseStream:
    """Base class for Stream objects; describes core supported operations"""

    def __init__(self, iterable):
        if iterable is None:
            raise NoneTypeError("Cannot create Stream from None")
        self._iterable = iterable
        self._is_consumed = False
        self._on_close_handler = None

    def __iter__(self):
        return iter(self.iterable)

    @property
    def iterable(self):
        if isinstance(self._iterable, Mapping):
            self._iterable = tuple(DictItem(k, v) for k, v in self._iterable.items())
        return self._iterable

    @iterable.setter
    def iterable(self, value):
        self._iterable = value

    def concat(self, *streams):
        """Concatenates several streams together or adds new streams/collections to the current one"""
        self.iterable = StreamGenerator.concat(self.iterable, *streams)
        return self

    def prepend(self, iterable):
        """Prepends iterable to current stream"""
        self.iterable = StreamGenerator.concat(iterable, self.iterable)
        return self

    def filter(self, predicate):
        """Filters values in stream based on given predicate function"""
        self.iterable = StreamGenerator.filter(self.iterable, predicate)
        return self

    def map(self, mapper):
        """Returns a stream consisting of the results of applying the given function to the elements of this stream"""
        self.iterable = StreamGenerator.map(self.iterable, mapper)
        return self

    def filter_map(self, mapper, *, discard_falsy=False):
        """Filters out all None or falsy values and applies mapper function to the elements of the stream"""
        self.iterable = StreamGenerator.filter_map(self.iterable, mapper, discard_falsy)
        return self

    def flat_map(self, mapper):
        """Maps each element of the stream and yields the elements of the produced iterators"""
        self.iterable = StreamGenerator.flat_map(self.iterable, mapper)
        return self

    def flatten(self):
        """Converts a Stream of multidimensional collection into a one-dimensional"""
        self.iterable = StreamGenerator.flatten(self.iterable)
        return self

    def peek(self, operation):
        """Performs the provided operation on each element of the stream without consuming it"""
        self.iterable = StreamGenerator.peek(self.iterable, operation)
        return self

    def distinct(self):
        """Returns a stream with the distinct elements of the current one"""
        self.iterable = StreamGenerator.distinct(self.iterable)
        return self

    @terminal
    def len(self):
        """Returns the count of elements in the stream"""
        return len(tuple(self.iterable))

    def _validate_numeric_data(self, op):
        data = tuple(self.iterable)
        if not all(isinstance(x, (int, float)) or x is None for x in data):
            raise ValueError(f"Cannot apply {op} on non-number elements")
        return [x for x in data if x is not None]

    @terminal
    def sum(self):
        """Sums the elements of the stream"""
        valid_data = self._validate_numeric_data(self.sum.__name__)
        return sum(valid_data) if valid_data else 0

    @terminal
    def average(self):
        """Returns the average value of elements in the stream"""
        valid_data = self._validate_numeric_data(self.average.__name__)
        return sum(valid_data) / len(valid_data) if valid_data else 0

    def skip(self, count):
        """Discards the first n elements of the stream and returns a new stream with the remaining ones"""
        if count < 0:
            raise ValueError("Skip count cannot be negative")
        self.iterable = StreamGenerator.skip(self.iterable, count)
        return self

    def limit(self, count):
        """Returns a stream with the first n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Limit count cannot be negative")
        self.iterable = StreamGenerator.limit(self.iterable, count)
        return self

    def head(self, count):
        """Alias for 'limit'"""
        if count < 0:
            raise ValueError("Head count cannot be negative")
        self.iterable = StreamGenerator.limit(self.iterable, count)
        return self

    def tail(self, count):
        """Returns a stream with the last n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Tail count cannot be negative")
        self.iterable = StreamGenerator.tail(self.iterable, count)
        return self

    def take_while(self, predicate):
        """Returns a stream that yields elements based on a predicate"""
        self.iterable = StreamGenerator.take_while(self.iterable, predicate)
        return self

    def drop_while(self, predicate):
        """Returns a stream that skips elements based on a predicate and yields the remaining ones"""
        self.iterable = StreamGenerator.drop_while(self.iterable, predicate)
        return self

    @terminal
    def take_first(self, default=None):
        """Returns Optional with the first element of the stream or a default value"""
        return Optional.of_nullable(next(iter(self.iterable), default))

    @terminal
    def take_last(self, default=None):
        """Returns Optional with the last element of the stream or a default value"""
        from collections import deque

        last_item = deque(self.iterable, maxlen=1)
        return Optional.of_nullable(last_item[0] if last_item else default)

    def sort(self, comparator=None, *, reverse=False):
        """
        Sorts the elements of the current stream according to natural order or based on the given comparator.
        If 'reverse' flag is True, the elements are sorted in descending order
        """
        self.iterable = StreamGenerator.sort(self.iterable, comparator, reverse)
        return self

    def reverse(self, comparator=None):
        """
        Sorts the elements of the current stream in descending order.
        Alias for 'sort(comparator, reverse=True)'
        """
        self.iterable = StreamGenerator.sort(self.iterable, comparator, reverse=True)
        return self

    @terminal
    def find_first(self, predicate=None):
        """
        Searches for an element of the stream that satisfies a predicate.
        Returns an Optional with the first found value, if any, or None
        """
        return Optional.of_nullable(next(filter(predicate, self.iterable), None))

    def any_match(self, predicate):
        """Returns whether any elements of the stream match the given predicate"""
        return any(predicate(i) for i in self.iterable)

    @terminal
    def all_match(self, predicate):
        """Returns whether all elements of the stream match the given predicate"""
        return all(predicate(i) for i in self.iterable)

    @terminal
    def none_match(self, predicate):
        """Returns whether no elements of the stream match the given predicate"""
        return not any(predicate(i) for i in self.iterable)

    @terminal
    def min(self, comparator=None, default=None):
        """Returns the minimum element of the stream according to the given comparator"""
        return Optional.of_nullable(min(self.iterable, key=comparator, default=default))

    @terminal
    def max(self, comparator=None, default=None):
        """Returns the maximum element of the stream according to the given comparator"""
        return Optional.of_nullable(max(self.iterable, key=comparator, default=default))

    @terminal
    def for_each(self, operation):
        """Performs an action for each element of this stream"""
        for i in self.iterable:
            operation(i)

    def enumerate(self, start=0):
        """
        Returns each element of the Stream preceded by his corresponding index
        (by default starting from 0 if not specified otherwise)
        """
        self.iterable = StreamGenerator.enumerate(self.iterable, start)
        return self

    @terminal
    def reduce(self, accumulator, identity=None):
        """
        Reduces the elements to a single one, by repeatedly applying a reducing operation.
        Returns Optional with the result, if any, or None
        """
        curr_iter = iter(self.iterable)
        try:
            if identity is None:
                identity = next(curr_iter)
        except StopIteration:
            return Optional.of_nullable(identity)

        for i in curr_iter:
            identity = accumulator(identity, i)
        return Optional.of_nullable(identity)

    @terminal
    def compare_with(self, other, comparator=None):
        """Compares current stream with another one based on a given comparator"""
        comparator = comparator or (lambda a, b: a == b)
        left, right = iter(self.iterable), iter(other)
        sentinel = object()
        while True:
            a = next(left, sentinel)
            b = next(right, sentinel)
            if a is sentinel and b is sentinel:
                return True
            if a is sentinel or b is sentinel:
                return False
            if not comparator(a, b):
                return False

    # ### collectors ###
    @terminal
    def collect(self, collection_type, dict_collector=None, dict_merger=None, str_delimiter=", "):
        """
        Returns a collection from the stream.

        In case of dict:
        The 'dict_collector' function receives an element from the stream and returns a (key, value) pair or a DictItem
        specifying how the dict should be constructed.

        The 'dict_merger' functions indicates in the case of a collision (duplicate keys), which entry should be kept.
        E.g. lambda old, new: new

        In case of str:
        Concatenates the elements of the Stream, separated by the specified 'str_delimiter'
        """
        import builtins

        match collection_type:
            case builtins.tuple:
                return self.to_tuple()
            case builtins.list:
                return self.to_list()
            case builtins.set:
                return self.to_set()
            case builtins.dict:
                return self.to_dict(dict_collector, dict_merger)
            case builtins.str:
                return self.to_string(str_delimiter)
            case _:
                raise ValueError("Invalid collection type")

    @terminal
    def to_list(self):
        """Returns a list of the elements of the current stream"""
        return list(self.iterable)

    @terminal
    def to_tuple(self):
        """Returns a tuple of the elements of the current stream"""
        return tuple(self.iterable)

    @terminal
    def to_set(self):
        """Returns a set of the elements of the current stream"""
        return set(self.iterable)

    @terminal
    def to_dict(self, collector=None, merger=None):
        """
        Returns a dict of the elements of the current stream.

        The 'collector' function receives an element from the stream and returns a (key, value) pair or a DictItem
        specifying how the dict should be constructed.

        The 'merger' functions indicates in the case of a collision (duplicate keys), which entry should be kept.
        E.g. lambda old, new: new
        """
        result = {}
        source = (collector(i) for i in self.iterable) if collector else self.iterable
        for item in source:
            k, v = self._unpack_dict_item(item)
            if k in result:
                if merger is None:
                    raise IllegalStateError(f"Key '{k}' already exists")
                v = merger(result[k], v)
            result[k] = v
        return result

    def _unpack_dict_item(self, item):  # noqa
        match item:
            case tuple():
                return item[0], item[1]
            case DictItem():
                # let's not make unnecessary calls to property getters
                return item._key, item._value  # noqa
            case _:
                raise UnsupportedTypeError(
                    f"Cannot create dict items from '{item.__class__.__name__}' type"
                )

    @terminal
    def to_string(self, delimiter=", "):
        """Concatenates the elements of the Stream, separated by the specified delimiter"""
        return self._join(delimiter)

    @terminal
    def group_by(self, classifier=None, collector=None):
        """
        Performs a "group by" operation on the elements of the stream according to a classification function.
        Returns the results in a dict built using collector function
        (optionally provided by the user or via a default one)
        """
        classifier = (lambda x: x) if classifier is None else classifier
        buckets = {}
        for item in self.iterable:
            key = classifier(item)
            buckets.setdefault(key, []).append(item)

        if collector is None:
            return buckets

        return dict(collector(k, v) for k, v in buckets.items())

    @terminal
    def quantify(self, predicate=bool):
        """Count how many of the elements are Truthy or evaluate to True based on a given predicate"""
        return sum(self.map(predicate))

    def close(self):
        """Closes the stream, causing the provided close handler to be called"""
        if self._is_consumed:
            return

        if self._on_close_handler:
            self._on_close_handler()
        self._is_consumed = True
        self._on_close_handler = None

    def on_close(self, handler):
        """Returns an equivalent stream with an additional close handler"""
        if not callable(handler):
            # since handler is called after stream result is materialized - check early on to avoid expensive computation
            raise TypeError(f"'{handler}' is not callable")
        self._on_close_handler = handler
        return self

    # ### let's look nice ###
    def __repr__(self):
        return f"{self.__class__.__name__}.of({self._join()})"

    def _join(self, delimiter=", "):
        return delimiter.join(str(i) for i in self.iterable)

from __future__ import annotations
from collections.abc import Mapping
from typing import Any, Callable, Generator, Iterable, Iterator, TypeVar, Self

from pyrio.iterators import StreamGenerator
from pyrio.decorators import handle_consumed, pre_call
from pyrio.utils import DictItem, Optional
from pyrio.exceptions import IllegalStateError, UnsupportedTypeError, NoneTypeError

T = TypeVar("T")
U = TypeVar("U")


@pre_call(handle_consumed)
class BaseStream:
    """Base class for Stream objects; describes core supported operations"""

    def __init__(self, iterable: Iterable[T]) -> None:
        if iterable is None:
            raise NoneTypeError("Cannot create Stream from None")
        self._iterable = iterable
        self._is_consumed = False
        self._on_close_handler: Callable[[], None] | None = None

    def __iter__(self) -> Iterator[Any]:
        return iter(self.iterable)

    @property
    def iterable(self) -> Iterable[Any]:
        if isinstance(self._iterable, Mapping):
            return (DictItem(k, v) for k, v in self._iterable.items())
        return self._iterable

    @iterable.setter
    def iterable(self, value: Iterable[Any]) -> None:
        self._iterable = value

    def concat(self, *streams: Iterable[Any]) -> Self:
        """Concatenates several streams together or adds new streams/collections to the current one"""
        self.iterable = StreamGenerator.concat(self.iterable, *streams)
        return self

    def prepend(self, iterable: Iterable[Any]) -> Self:
        """Prepends iterable to current stream"""
        self.iterable = StreamGenerator.concat(iterable, self.iterable)
        return self

    def filter(self, predicate: Callable[[Any], bool]) -> Self:
        """Filters values in stream based on given predicate function"""
        self.iterable = StreamGenerator.filter(self.iterable, predicate)
        return self

    def map(self, mapper: Callable[[Any], Any]) -> Self:
        """Returns a stream consisting of the results of applying the given function to the elements of this stream"""
        self.iterable = StreamGenerator.map(self.iterable, mapper)
        return self

    def filter_map(self, mapper: Callable[[Any], Any], *, discard_falsy: bool = False) -> Self:
        """Filters out all None or falsy values and applies mapper function to the elements of the stream"""
        self.iterable = StreamGenerator.filter_map(self.iterable, mapper, discard_falsy)
        return self

    def flat_map(self, mapper: Callable[[Any], Iterable[Any]]) -> Self:
        """Maps each element of the stream and yields the elements of the produced iterators"""
        self.iterable = StreamGenerator.flat_map(self.iterable, mapper)
        return self

    def flatten(self) -> Self:
        """Converts a Stream of multidimensional collection into a one-dimensional"""
        self.iterable = StreamGenerator.flatten(self.iterable)
        return self

    def peek(self, operation: Callable[[Any], Any]) -> Self:
        """Performs the provided operation on each element of the stream without consuming it"""
        self.iterable = StreamGenerator.peek(self.iterable, operation)
        return self

    def distinct(self) -> Self:
        """Returns a stream with the distinct elements of the current one"""
        self.iterable = StreamGenerator.distinct(self.iterable)
        return self

    def count(self) -> int:
        """Returns the count of elements in the stream"""
        return len(list(self.iterable))

    def sum(self) -> Any:
        """Sums the elements of the stream"""
        items = list(self.iterable)
        if len(items) == 0:
            return 0
        if not any(isinstance(x, (int, float)) or x is None for x in items):
            raise ValueError("Cannot apply sum on non-number elements")
        return sum(items)

    def average(self) -> Any:
        """Returns the average value of elements in the stream"""
        items = list(self.iterable)
        if (stream_len := len(items)) == 0:
            return 0
        self._iterable = items  # Update iterable with computed list
        return self.sum() / stream_len

    def skip(self, count: int) -> Self:
        """Discards the first n elements of the stream and returns a new stream with the remaining ones"""
        if count < 0:
            raise ValueError("Skip count cannot be negative")
        self.iterable = StreamGenerator.skip(self.iterable, count)
        return self

    def limit(self, count: int) -> Self:
        """Returns a stream with the first n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Limit count cannot be negative")
        self.iterable = StreamGenerator.limit(self.iterable, count)
        return self

    def head(self, count: int) -> Self:
        """Alias for 'limit'"""
        if count < 0:
            raise ValueError("Head count cannot be negative")
        self.iterable = StreamGenerator.limit(self.iterable, count)
        return self

    def tail(self, count: int) -> Self:
        """Returns a stream with the last n elements, or fewer if the underlying iterator ends sooner"""
        if count < 0:
            raise ValueError("Tail count cannot be negative")
        self.iterable = StreamGenerator.tail(self.iterable, count)
        return self

    def take_while(self, predicate: Callable[[Any], bool]) -> Self:
        """Returns a stream that yields elements based on a predicate"""
        self.iterable = StreamGenerator.take_while(self.iterable, predicate)
        return self

    def drop_while(self, predicate: Callable[[Any], bool]) -> Self:
        """Returns a stream that skips elements based on a predicate and yields the remaining ones"""
        self.iterable = StreamGenerator.drop_while(self.iterable, predicate)
        return self

    def take_first(self, default: Any = None) -> Optional:
        """Returns Optional with the first element of the stream or a default value"""
        return Optional.of_nullable(next(iter(self.iterable), default))

    def take_last(self, default: Any = None) -> Optional:
        """Returns Optional with the last element of the stream or a default value"""
        if self.iterable:
            *_, last = self.iterable
            return Optional.of_nullable(last)
        return Optional.of_nullable(default)

    def sort(
        self, comparator: Callable[[Any], Any] | None = None, *, reverse: bool = False
    ) -> Self:
        """
        Sorts the elements of the current stream according to natural order or based on the given comparator.
        If 'reverse' flag is True, the elements are sorted in descending order
        """
        self.iterable = StreamGenerator.sort(self.iterable, comparator, reverse)
        return self

    def reverse(self, comparator: Callable[[Any], Any] | None = None) -> Self:
        """
        Sorts the elements of the current stream in descending order.
        Alias for 'sort(comparator, reverse=True)'
        """
        self.iterable = StreamGenerator.sort(self.iterable, comparator, reverse=True)
        return self

    def find_first(self, predicate: Callable[[Any], bool] | None = None) -> Optional:
        """
        Searches for an element of the stream that satisfies a predicate.
        Returns an Optional with the first found value, if any, or None
        """
        return Optional.of_nullable(next(filter(predicate, self.iterable), None))

    def find_any(self, predicate: Callable[[Any], bool] | None = None) -> Optional:
        """
        Searches for an element of the stream that satisfies a predicate.
        Returns an Optional with some of the found values, if any, or None
        """
        import random

        if predicate:
            self.filter(predicate)
        try:
            return Optional.of(random.choice(list(self.iterable)))
        except IndexError:
            return Optional.of_nullable(None)

    def any_match(self, predicate: Callable[[Any], bool]) -> bool:
        """Returns whether any elements of the stream match the given predicate"""
        return any(predicate(i) for i in self.iterable)

    def all_match(self, predicate: Callable[[Any], bool]) -> bool:
        """Returns whether all elements of the stream match the given predicate"""
        return all(predicate(i) for i in self.iterable)

    def none_match(self, predicate: Callable[[Any], bool]) -> bool:
        """Returns whether no elements of the stream match the given predicate"""
        return any(not predicate(i) for i in self.iterable)

    def min(self, comparator: Callable[[Any], Any] | None = None, default: Any = None) -> Optional:
        """Returns the minimum element of the stream according to the given comparator"""
        return Optional.of_nullable(min(self.iterable, key=comparator, default=default))

    def max(self, comparator: Callable[[Any], Any] | None = None, default: Any = None) -> Optional:
        """Returns the maximum element of the stream according to the given comparator"""
        return Optional.of_nullable(max(self.iterable, key=comparator, default=default))

    def for_each(self, operation: Callable[[Any], Any]) -> None:
        """Performs an action for each element of this stream"""
        for i in self.iterable:
            operation(i)

    def enumerate(self, start: int = 0) -> Self:
        """
        Returns each element of the Stream preceded by his corresponding index
        (by default starting from 0 if not specified otherwise)
        """
        self.iterable = StreamGenerator.enumerate(self.iterable, start)
        return self

    def reduce(self, accumulator: Callable[[Any, Any], Any], identity: Any = None) -> Optional:
        """
        Reduces the elements to a single one, by repeatedly applying a reducing operation.
        Returns Optional with the result, if any, or None
        """
        items = list(self.iterable)
        if len(items) == 0:
            return Optional.of_nullable(identity)

        curr_iter = iter(items)
        if identity is None:
            identity = next(curr_iter)

        for i in curr_iter:
            identity = accumulator(identity, i)

        # Update the iterable to maintain consistency
        self._iterable = items
        return Optional.of_nullable(identity)

    def compare_with(
        self, other: Iterable[Any], comparator: Callable[[Any, Any], bool] | None = None
    ) -> bool:
        """Compares current stream with another one based on a given comparator"""
        items = list(self.iterable)
        self._iterable = items  # Update iterable after consumption
        return not any(
            (comparator and not comparator(i, j)) or i != j for i, j in zip(items, other)
        )

    # ### collectors ###
    def collect(
        self,
        collection_type: type[list[Any]]
        | type[tuple[Any, ...]]
        | type[set[Any]]
        | type[dict[Any, Any]]
        | type[str],
        dict_collector: Callable[[Any], tuple[Any, Any] | DictItem] | None = None,
        dict_merger: Callable[[Any, Any], Any] | None = None,
        str_delimiter: str = ", ",
    ) -> list[Any] | tuple[Any, ...] | set[Any] | dict[Any, Any] | str:
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

    def to_list(self) -> list[Any]:
        """Returns a list of the elements of the current stream"""
        return list(self.iterable)

    def to_tuple(self) -> tuple[Any, ...]:
        """Returns a tuple of the elements of the current stream"""
        return tuple(self.iterable)

    def to_set(self) -> set[Any]:
        """Returns a set of the elements of the current stream"""
        return set(self.iterable)

    def to_dict(
        self,
        collector: Callable[[Any], tuple[Any, Any] | DictItem] | None = None,
        merger: Callable[[Any, Any], Any] | None = None,
    ) -> dict[Any, Any]:
        """
        Returns a dict of the elements of the current stream.

        The 'collector' function receives an element from the stream and returns a (key, value) pair or a DictItem
        specifying how the dict should be constructed.

        The 'merger' functions indicates in the case of a collision (duplicate keys), which entry should be kept.
        E.g. lambda old, new: new
        """
        result: dict[Any, Any] = {}
        source = (collector(i) for i in self.iterable) if collector else self.iterable
        for item in source:
            k, v = self._unpack_dict_item(item)
            if k in result:
                if merger is None:
                    raise IllegalStateError(f"Key '{k}' already exists")
                v = merger(result[k], v)
            result[k] = v
        return result

    def _unpack_dict_item(self, item: tuple[Any, Any] | DictItem) -> tuple[Any, Any]:  # noqa
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

    def to_string(self, delimiter: str = ", ") -> str:
        """Concatenates the elements of the Stream, separated by the specified delimiter"""
        return self._join(delimiter)

    def group_by(
        self,
        classifier: Callable[[Any], Any] | None = None,
        collector: Callable[[Any, list[Any]], tuple[Any, Any]] | None = None,
    ) -> dict[Any, list[Any]] | dict[Any, Any]:
        """
        Performs a "group by" operation on the elements of the stream according to a classification function.
        Returns the results in a dict built using collector function
        (optionally provided by the user or via a default one)
        """
        if collector is None:
            return {key: list(group) for key, group in self._group_by(classifier)}

        result: dict[Any, Any] = {}
        for key, group in self._group_by(classifier):
            key, group = collector(key, list(group))
            if hasattr(group, "__iter__"):
                if key not in result:
                    result[key] = []
                result[key] += group
            else:
                result[key] = group
        return result

    def _group_by(
        self, classifier: Callable[[Any], Any] | None = None
    ) -> Generator[tuple[Any, Generator[Any]]]:
        # https://docs.python.org/3/library/itertools.html#itertools.groupby
        classifier = (lambda x: x) if classifier is None else classifier
        iterator = iter(self.iterable)
        exhausted = False
        curr_value: Any = None
        curr_key: Any = None

        def _grouper(target_key: Any) -> Generator[Any]:  # noqa
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

    def quantify(self, predicate: Callable[[Any], bool] = bool) -> int:
        """Count how many of the elements are Truthy or evaluate to True based on a given predicate"""
        return sum(self.map(predicate))

    def close(self) -> None:
        """Closes the stream, causing the provided close handler to be called"""
        if self._on_close_handler:
            self._on_close_handler()
        self._is_consumed = True

    def on_close(self, handler: Callable[[], None]) -> Self:
        """Returns an equivalent stream with an additional close handler"""
        self._on_close_handler = handler
        return self

    # ### let's look nice ###
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.of({self._join()})"

    def _join(self, delimiter: str = ", ") -> str:
        return delimiter.join(str(i) for i in self.iterable)

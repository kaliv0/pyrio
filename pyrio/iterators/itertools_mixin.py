from __future__ import annotations
import itertools as it
import operator
from typing import Any, Callable, Generator, Iterable, TypeVar, Self

from pyrio.utils import Optional

T = TypeVar("T")
U = TypeVar("U")


class ItertoolsMixin:
    iterable: Any

    def use(self, it_function: Callable[..., Any], **kwargs: Any) -> Self:
        """Provides integration with itertools methods; pass corresponding parameters as kwargs"""
        import inspect

        if self._handle_no_signature_functions(it_function, **kwargs):
            return self

        signature = inspect.signature(it_function).parameters
        if self._handle_no_kwargs_functions(signature, it_function, **kwargs):
            return self

        return self._handle_default_signature_functions(signature, it_function, **kwargs)

    def _handle_no_signature_functions(
        self, it_function: Callable[..., Any], **kwargs: Any
    ) -> bool:
        NO_SIGNATURE_FUNCTIONS = ["chain", "islice", "product", "repeat", "zip_longest"]

        if it_function.__name__ not in NO_SIGNATURE_FUNCTIONS:
            return False

        if it_function.__name__ in ("product", "zip_longest"):
            if isinstance(self.iterable, range):
                self.iterable = it_function(self.iterable, **kwargs)
            else:
                self.iterable = it_function(*self.iterable, **kwargs)
            return True

        # functions like 'chain' don't expect key-word arguments
        self.iterable = it_function(self.iterable, *kwargs.values())
        return True

    def _handle_no_kwargs_functions(
        self, signature: Any, it_function: Callable[..., Any], **kwargs: Any
    ) -> bool:
        NO_KWARGS_FUNCTIONS = ["dropwhile", "filterfalse", "starmap", "takewhile", "tee"]

        # handle functions that take only iterable as arg
        if len(signature.keys()) == 1 and "iterable" in signature:
            self.iterable = it_function(self.iterable)
            return True

        # handle functions that take no kwargs
        if it_function.__name__ in NO_KWARGS_FUNCTIONS:
            if it_function.__name__ == "tee":
                self.iterable = it_function(self.iterable, *kwargs.values())
            else:
                self.iterable = it_function(*kwargs.values(), self.iterable)
            return True
        return False

    def _handle_default_signature_functions(
        self, signature: Any, it_function: Callable[..., Any], **kwargs: Any
    ) -> Self:
        if "iterable" in signature:
            kwargs["iterable"] = self.iterable
        elif "data" in signature:
            kwargs["data"] = self.iterable
        self.iterable = it_function(**kwargs)
        return self

    # ### 'recipes' ###
    # https://docs.python.org/3/library/itertools.html#itertools-recipes
    def tabulate(self, mapper: Callable[[int], T], start: int = 0) -> Self:
        """Returns function(0), function(1), ..."""
        self.iterable = map(mapper, it.count(start))
        return self

    def repeat_func(self, operation: Callable[..., T], times: int | None = None) -> Self:
        """Repeats calls to func with specified arguments"""
        self.iterable = it.starmap(operation, it.repeat(self.iterable, times or 1))
        return self

    def ncycles(self, count: int = 0) -> Self:
        """Returns the stream elements n times"""
        self.iterable = it.chain.from_iterable(it.repeat(tuple(self.iterable), count))
        return self

    def consume(self, n: int | None = None) -> Self:
        """Advances the iterator n-steps ahead. If n is None, consumes stream entirely"""
        import collections

        if n is None:
            self.iterable = collections.deque(self.iterable, maxlen=0)
            return self
        if n < 0:
            raise ValueError("Consume boundary cannot be negative")
        self.iterable = it.islice(self.iterable, n, len(self.iterable))
        return self

    def take_nth(self, idx: int, default: Any = None) -> Optional:
        """Returns Optional with the nth element of the stream or a default value"""
        if idx < 0:
            idx = len(self.iterable) + idx
        return Optional.of_nullable(next(it.islice(self.iterable, idx, None), default))

    def all_equal(self, key: Callable[[T], Any] | None = None) -> bool:
        """Returns True if all elements of the stream are equal to each other"""
        return len(list(it.islice(it.groupby(self.iterable, key), 2))) <= 1

    def view(self, start: int = 0, stop: int | None = None, step: int | None = None) -> Self:
        """Provides access to a selected part of the stream"""
        if start < 0:
            start = len(self.iterable) + start

        if stop and stop < 0:
            stop = len(self.iterable) + stop

        if step and step < 0:
            raise ValueError("Step must be a positive integer or None")

        self.iterable = it.islice(self.iterable, start, stop, step)
        return self

    # ### unique ###
    def unique(self, key: Callable[[T], Any] | None = None, reverse: bool = False) -> Self:
        """Yields unique elements in sorted order. Supports unhashable inputs"""
        self.iterable = self._unique(sorted(self.iterable, key=key, reverse=reverse), key=key)
        return self

    @staticmethod
    def _unique(iterable: Iterable[T], key: Callable[[T], Any] | None = None) -> map[T]:
        return map(next, map(operator.itemgetter(1), it.groupby(iterable, key)))

    def unique_just_seen(self, key: Callable[[T], Any] | None = None) -> Self:
        """Yields unique elements, preserving order. Remembers only the element just seen"""
        self.iterable = map(next, map(operator.itemgetter(1), it.groupby(self.iterable, key)))
        return self

    def unique_ever_seen(self, key: Callable[[T], Any] | None = None) -> Self:
        """Yields unique elements, preserving order. Remembers all elements ever seen"""
        self.iterable = self._unique_ever_seen(self.iterable, key)
        return self

    @staticmethod
    def _unique_ever_seen(
        iterable: Iterable[T], key: Callable[[T], Any] | None = None
    ) -> Generator[T]:
        seen = set()
        for element in iterable:
            k = key(element) if key else element
            if k not in seen:
                seen.add(k)
                yield element

    # ### ###
    def sliding_window(self, n: int) -> Self:
        """Collects data into overlapping fixed-length chunks or blocks"""
        if n < 0:
            raise ValueError("Window size cannot be negative")
        self.iterable = self._sliding_window(self.iterable, n)
        return self

    @staticmethod
    def _sliding_window(iterable: Iterable[T], n: int) -> Generator[tuple[T, ...]]:
        import collections

        window = collections.deque(it.islice(iterable, n - 1), maxlen=n)
        for x in it.islice(iterable, n - 1, None):
            window.append(x)
            yield tuple(window)

    def grouper(self, n: int, *, incomplete: str = "fill", fill_value: Any = None) -> Self:
        """Collects data into non-overlapping fixed-length chunks or blocks"""
        self.iterable = self._grouper(n, incomplete, fill_value)
        return self

    def _grouper(
        self, n: int, incomplete: str = "fill", fill_value: Any = None
    ) -> zip[tuple[T, ...]] | it.zip_longest[tuple[T, ...]] | Generator[tuple[T, ...]]:
        iterators = [iter(self.iterable)] * n
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

    def round_robin(self) -> Self:
        """Visits input iterables in a cycle until each is exhausted"""
        self.iterable = self._round_robin(self.iterable)
        return self

    @staticmethod
    def _round_robin(iterable: Iterable[Iterable[T]]) -> Generator[T]:
        # Algorithm credited to George Sakkis
        iterable_list = list(iterable)
        iterators = map(iter, iterable_list)
        for num_active in range(len(iterable_list), 0, -1):
            iterators = it.cycle(it.islice(iterators, num_active))  # type: ignore
            yield from map(next, iterators)

    def partition(self, predicate: Callable[[T], bool]) -> Self:
        """
        Partitions entries into true and false entries.
        Returns a stream of two nested generators
        """
        true_iter, false_iter = it.tee(self.iterable)
        self.iterable = filter(predicate, true_iter), it.filterfalse(predicate, false_iter)
        return self

    def subslices(self) -> Self:
        """Returns all contiguous non-empty sub-slices"""
        slices = it.starmap(slice, it.combinations(range(len(self.iterable) + 1), 2))
        self.iterable = map(operator.getitem, it.repeat(self.iterable), slices)  # noqa
        return self

    def find_indices(self, value: T, start: int = 0, stop: int | None = None) -> Self:
        """Returns indices where a value occurs in a sequence or iterable"""
        self.iterable = self._find_indices(self.iterable, value, start, stop)
        return self

    @staticmethod
    def _find_indices(
        iterable: Iterable[T], value: T, start: int = 0, stop: int | None = None
    ) -> Generator[int]:
        iterator = it.islice(iterable, start, stop)
        for i, element in enumerate(iterator, start):
            if element is value or element == value:
                yield i

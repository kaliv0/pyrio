from __future__ import annotations
from collections.abc import Iterable
from typing import Any, Callable, Generator, TypeVar

from pyrio.decorators import map_dict_items

T = TypeVar("T")
U = TypeVar("U")


class StreamGenerator:
    @staticmethod
    @map_dict_items
    def concat(*streams: Iterable[T]) -> Generator[T]:
        for iterable in streams:
            yield from iterable

    @staticmethod
    def filter(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Generator[T]:
        for i in iterable:
            if predicate(i):
                yield i

    @staticmethod
    def map(iterable: Iterable[T], mapper: Callable[[T], U]) -> Generator[U]:
        for i in iterable:
            yield mapper(i)

    @staticmethod
    def filter_map(
        iterable: Iterable[T], mapper: Callable[[T], U], discard_falsy: bool = False
    ) -> Generator[U]:
        for i in iterable:
            if (not discard_falsy and i is not None) or (discard_falsy and i):
                yield mapper(i)

    @staticmethod
    def flat_map(iterable: Iterable[T], mapper: Callable[[T], Iterable[U]]) -> Generator[U]:
        for i in iterable:
            yield from mapper(i)

    @classmethod
    def flatten(cls, iterable: Iterable[Any]) -> Generator[Any]:
        for i in iterable:
            if isinstance(i, str) or not isinstance(i, Iterable):
                yield i
            else:
                yield from cls.flatten(i)

    @staticmethod
    def peek(iterable: Iterable[T], operation: Callable[[T], Any]) -> Generator[T]:
        for i in iterable:
            operation(i)
            yield i

    @staticmethod
    def iterate(
        seed: T, operation: Callable[[T], T], condition: Callable[[T], bool] | None = None
    ) -> Generator[T]:
        if condition is None:
            condition = lambda _: True  # noqa
        while condition(seed):
            yield seed
            seed = operation(seed)

    @staticmethod
    def generate(supplier: Callable[[], T]) -> Generator[T]:
        while True:
            yield supplier()

    @staticmethod
    def range(start: int, stop: int, step: int = 1) -> Generator[int]:
        for i in range(start, stop, step):
            yield i

    @staticmethod
    def distinct(iterable: Iterable[T]) -> Generator[T]:
        elements: set[T] = set()
        for i in iterable:
            if i not in elements:
                elements.add(i)
                yield i

    @staticmethod
    def skip(iterable: Iterable[T], count: int) -> Generator[T]:
        for i in iterable:
            if count > 0:
                count -= 1
            else:
                yield i

    @staticmethod
    def limit(iterable: Iterable[T], count: int) -> Generator[T]:
        for i in iterable:
            if count == 0:
                break
            yield i
            count -= 1

    @staticmethod
    def tail(iterable: Iterable[T], count: int) -> Generator[T]:
        import collections

        for i in collections.deque(iterable, maxlen=count):
            yield i

    @staticmethod
    def take_while(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Generator[T]:
        for i in iterable:
            if not predicate(i):
                break
            yield i

    @staticmethod
    def drop_while(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Generator[T]:
        iterator = iter(iterable)
        for x in iterator:
            if not predicate(x):
                yield x
                break

        for x in iterator:
            yield x

    @staticmethod
    def sort(
        iterable: Iterable[T], comparator: Callable[[T], Any] | None = None, reverse: bool = False
    ) -> Generator[T]:
        for i in sorted(iterable, key=comparator, reverse=reverse):  # type: ignore
            yield i

    @staticmethod
    def enumerate(iterable: Iterable[T], start: int = 0) -> Generator[tuple[int, T]]:
        for i, item in enumerate(iterable, start):
            yield i, item

from __future__ import annotations
from typing import Any, Callable, TypeVar

from pyrio.exceptions import NoSuchElementError, NoneTypeError

T = TypeVar("T")


class Optional:
    """Container object which may (or may not) contain a non-null value"""

    def __init__(self, element: T | None) -> None:
        self._element = element

    def __str__(self) -> str:
        return f"Optional[{self._element}]"

    @staticmethod
    def empty() -> Optional:
        """Creates empty Optional"""
        return Optional(None)

    @staticmethod
    def of(element: T) -> Optional:
        """Creates Optional describing given non-null value"""
        if element is None:
            raise NoneTypeError("Value cannot be None")
        return Optional(element)

    @staticmethod
    def of_nullable(element: T | None) -> Optional:
        """
        Returns an Optional describing the given value, if non-null,
        otherwise returns an empty Optional
        """
        return Optional(element)

    def get(self) -> Any:
        """If a value is present, returns the value, otherwise raises an Exception"""
        if self.is_empty():
            raise NoSuchElementError("Optional is empty")
        return self._element

    def is_present(self) -> bool:
        """Returns bool whether a value is present"""
        return not self.is_empty()

    def is_empty(self) -> bool:
        """Returns bool whether the Optional is empty"""
        return self._element is None

    def if_present(self, action: Callable[[T], None]) -> None:
        """Performs given action with the value if the Optional is not empty"""
        if self.is_present():
            action(self.get())

    def if_present_or_else(
        self, action: Callable[[T], None], empty_action: Callable[[], None]
    ) -> None:
        """Performs given action with the value if the Optional is not empty,
        otherwise calls fallback 'empty_action'
        """
        if self.is_present():
            action(self.get())
        else:
            empty_action()

    def or_else(self, value: Any) -> Any:
        """
        Returns the value if present, or a provided argument otherwise.
        Safe alternative to get() method
        """
        return self._element if self.is_present() else value

    def or_else_get(self, supplier: Callable[[], Any]) -> Any:
        """
        Returns the value if present, or calls a 'supplier' function otherwise.
        Safe alternative to get() method
        """
        return self._element if self.is_present() else supplier()

    def or_else_raise(self, supplier: Callable[[], None] | None = None) -> Any:
        """
        Returns the value if present,
        otherwise throws an exception produced by the exception supplying function
        (if such is provided by the user) or NoSuchElementError
        """
        if supplier is None:

            def supplier() -> None:
                raise NoSuchElementError("Optional is empty")

        return self._element if self.is_present() else supplier()

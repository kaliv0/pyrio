from pyrio.exceptions import NoneTypeError, NoSuchElementError


class Optional:
    """Container object which may (or may not) contain a non-null value"""

    def __init__(self, element):
        self._element = element

    @staticmethod
    def empty():
        """Creates empty Optional"""
        return Optional(None)

    @staticmethod
    def of(element):
        """Creates Optional describing given non-null value"""
        if element is None:
            raise NoneTypeError("Value cannot be None")
        return Optional(element)

    @staticmethod
    def of_nullable(element):
        """
        Returns an Optional describing the given value, if non-null,
        otherwise returns an empty Optional
        """
        return Optional(element)

    def get(self):
        """If a value is present, returns the value, otherwise raises an Exception"""
        if self.is_empty():
            raise NoSuchElementError("Optional is empty")
        return self._element

    def is_present(self):
        """Returns bool whether a value is present"""
        return not self.is_empty()

    def is_empty(self):
        """Returns bool whether the Optional is empty"""
        return self._element is None

    def if_present(self, action):
        """Performs given action with the value if the Optional is not empty"""
        if self.is_present():
            action(self.get())

    def if_present_or_else(self, action, empty_action):
        """
        Performs given action with the value if the Optional is not empty,
        otherwise calls fallback 'empty_action'
        """
        if self.is_present():
            action(self.get())
        else:
            empty_action()

    def or_else(self, value):
        """
        Returns the value if present, or a provided argument otherwise.
        Safe alternative to get() method
        """
        return self._element if self.is_present() else value

    def or_else_get(self, supplier):
        """
        Returns the value if present, or calls a 'supplier' function otherwise.
        Safe alternative to get() method
        """
        return self._element if self.is_present() else supplier()

    def or_else_optional(self, supplier):
        """
        Returns this Optional if a value is present,
        otherwise returns an Optional produced by the supplier
        """
        if self.is_present():
            return self
        result = supplier()
        if not isinstance(result, Optional):
            raise TypeError(f"{result} is not an Optional")
        return result

    def or_else_raise(self, supplier=None):
        """
        Returns the value if present,
        otherwise throws an exception produced by the exception supplying function
        (if such is provided by the user) or NoSuchElementError
        """
        if self.is_present():
            return self._element
        if supplier is None:
            raise NoSuchElementError("Optional is empty")
        raise supplier()

    def filter(self, predicate):
        """
        If a value is present and matches the predicate, returns this Optional.
        Otherwise returns an empty Optional.
        """
        if self.is_empty() or not predicate(self.get()):
            return Optional.empty()
        return self

    def map(self, mapper):
        """
        If a value is present, applies the mapper and returns an Optional of the result.
        Returns an empty Optional if this Optional is empty or the mapper returns None.
        """
        if self.is_empty():
            return Optional.empty()
        return Optional.of_nullable(mapper(self.get()))

    def flat_map(self, mapper):
        """
        If a value is present, applies the mapper and returns the resulting Optional.
        Returns an empty Optional if this Optional is empty.
        """
        if self.is_empty():
            return Optional.empty()
        result = mapper(self.get())
        if not isinstance(result, Optional):
            raise TypeError(f"{result} is not an Optional")
        return result

    def to_stream(self):
        """
        Returns a Stream with the value if present, otherwise an empty Stream.
        """
        from pyrio.streams import Stream

        if self.is_empty():
            return Stream.empty()
        return Stream.of(self.get())

    def __repr__(self):
        return "Optional.empty" if self.is_empty() else f"Optional[{self._element}]"

    def __eq__(self, other):
        if not isinstance(other, Optional):
            raise TypeError(f"{other} is not an Optional")
        return self._element == other._element

    def __hash__(self):
        try:
            return hash(self._element)
        except TypeError as e:
            raise TypeError(
                f"unhashable type: 'Optional' (value of type '{type(self._element).__name__}' is unhashable)"
            ) from e

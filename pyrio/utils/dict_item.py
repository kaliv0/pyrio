from collections.abc import Mapping
from operator import attrgetter


class DictItem:
    """Helper record class for mapping key-value pairs."""

    __match_args__ = ("key", "value")
    key_of = attrgetter("key")
    value_of = attrgetter("value")

    def __init__(self, key, value):
        self._key = key
        self._value = value

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._map(self._value)

    def with_key(self, key):
        """Return a new DictItem with the same value and a different key."""
        return DictItem(key, self._value)

    def with_value(self, value):
        """Return a new DictItem with the same key and a different value."""
        return DictItem(self._key, value)

    @staticmethod
    def replace_null(default):
        """Return a handler that replaces None values with given default."""
        return lambda item: item.with_value(default) if item.value is None else item

    def entries(self):
        """
        Return a Stream of DictItems for a nested mapping value.

        Equivalent to Stream(item.value) when value is (or maps to) mapping entries.
        Raises TypeError if the stored value is not a mapping.
        """
        if not isinstance(self._value, Mapping):
            raise TypeError(f"entries() expects a mapping value, got '{type(self._value).__name__}' instead")
        from pyrio.streams import Stream

        return Stream(self.value)

    def _map(self, val):
        if isinstance(val, dict):
            return tuple(DictItem(k, self._map(v)) for k, v in val.items())
        return val

    def __iter__(self):
        yield self.key
        yield self.value

    def __repr__(self):
        key = f"{self.key}" if isinstance(self.key, str) else self.key
        value = f"{self.value}" if isinstance(self.value, str) else self.value
        return f"DictItem({key=}, {value=})"

    def __eq__(self, other):
        if not isinstance(other, DictItem):
            raise TypeError(f"{other} is not a DictItem")
        return self._key == other._key and self._value == other._value  # noqa

    def __hash__(self):
        try:
            return hash((self._key, self._value))
        except TypeError as e:
            raise TypeError(
                f"unhashable type: 'DictItem' (value of type '{type(self._value).__name__}' is unhashable)"
            ) from e

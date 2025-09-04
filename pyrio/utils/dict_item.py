from __future__ import annotations
from typing import Any, Union


class DictItem:
    """Helper record class for mapping key-value pairs"""

    def __init__(self, key: Any, value: Any) -> None:
        self._key = key
        self._value = value

    @property
    def key(self) -> Any:
        return self._key

    @property
    def value(self) -> Union[tuple[DictItem, ...], Any]:
        return self._map(self._value)

    def _map(self, val: Any) -> Union[tuple[DictItem, ...], Any]:
        if isinstance(val, dict):
            return tuple(DictItem(k, self._map(v)) for k, v in val.items())
        return val

    def __repr__(self) -> str:
        key = f"{self.key}" if isinstance(self.key, str) else self.key
        value = f"{self.value}" if isinstance(self.value, str) else self.value
        return f"DictItem({key=}, {value=})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DictItem):
            raise TypeError(f"{other} is not a DictItem")
        return bool(self._key == other._key and self._value == other._value)  # noqa

    def __hash__(self) -> int:
        return hash((self._key, self._value))

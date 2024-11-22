from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Item:
    key: Any
    value: Any

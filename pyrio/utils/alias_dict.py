from collections import UserDict
from itertools import chain

from pyrio.exceptions import AliasError


class AliasDict(UserDict):
    # TODO: docstring
    _VAL_DICT = {}

    def add_alias(self, key, alias):
        if alias == key:
            raise AliasError("Key and corresponding alias cannot be equal")
        self._VAL_DICT[alias] = key

    def iterkeys(self):
        # TODO: hashset in cases when alias and key are the same
        return tuple(chain(self.keys(), self._VAL_DICT.keys()))

    # TODO: do we need this?
    def itervalues(self):
        return tuple(self.values())

    def iteritems(self):
        return tuple(
            (k, v)
            for k, v in chain(
                self.items(), ((k, self[k]) for k in self._VAL_DICT.keys() if k not in self.keys())
            )
        )

    def __missing__(self, key):
        try:
            return super().__getitem__(self._VAL_DICT[key])
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        try:
            key = self._VAL_DICT[key]
        except KeyError:
            pass
        super().__setitem__(key, value)

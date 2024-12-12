from collections import UserDict
from itertools import chain

from pyrio.exceptions import AliasError

# TODO: use self.data instead of self.keys()/.items() etc??


class AliasDict(UserDict):
    """Custom Dict class supporting key-aliases pointing to shared values"""

    def __init__(self, dict_):
        self._VAL_DICT = {}
        super().__init__(self, **dict_)

    def add_alias(self, key, alias):
        if alias == key:
            raise AliasError("Key and corresponding alias cannot be equal")
        if key not in self.keys():
            raise KeyError(f"Key '{key}' not found")
        self._VAL_DICT[alias] = key

    def remove_alias(self, alias):
        # TODO: raise if not found?
        self._VAL_DICT.pop(alias, None)

    # TODO: handle if key is removed -> cascade_delete all aliases -> add test

    def iterkeys(self):
        return tuple(chain(self.keys(), self._VAL_DICT.keys()))

    # TODO: do we need this?
    def itervalues(self):
        return tuple(self.values())

    def iteritems(self):
        return tuple(
            (k, v) for k, v in chain(self.items(), ((k, self[v]) for k, v in self._VAL_DICT.items()))
        )

    def __missing__(self, key):
        try:
            return super().__getitem__(self._VAL_DICT[key])
        except AttributeError:
            raise KeyError(f"Key '{key}' not found")

    def __setitem__(self, key, value):
        try:
            key = self._VAL_DICT[key]
        except KeyError:
            pass
        super().__setitem__(key, value)

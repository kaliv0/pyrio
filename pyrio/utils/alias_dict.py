from collections import UserDict

from pyrio.exceptions import AliasError


class AliasDict(UserDict):
    """Custom Dict class supporting key-aliases pointing to shared values"""

    def __init__(self, dict_):
        self._VAL_DICT = {}
        super().__init__(self, **dict_)

    def add_alias(self, key, alias):
        if alias == key:
            raise AliasError(f"Key and corresponding alias cannot be equal: '{key}'")
        if key not in self.data.keys():
            # raise KeyError(f"Key '{key}' not found")
            raise KeyError(key)
        self._VAL_DICT[alias] = key

    # TODO: add bulk delete -> remove_aliases(*alias_list)
    def remove_alias(self, alias):
        self._VAL_DICT.__delitem__(alias)

        # if self._VAL_DICT.pop(alias, None) is None:
        #   # TODO: use custom error -> AliasNotFound?
        # raise KeyError(f"Alias '{alias}' not found")

    def aliases(self):
        return self._VAL_DICT.keys()

    # TODO: show keys only -> add method?

    def keys(self):
        # return tuple(chain(self.data.keys(), self._VAL_DICT.keys()))

        combined_dict = {**self.data, **self._VAL_DICT}
        return combined_dict.keys()

    # TODO: do we need this?
    def values(self):
        return self.data.values()

    def items(self):
        # return tuple(
        #     (k, v) for k, v in chain(self.data.items(), ((k, self[v]) for k, v in self._VAL_DICT.items()))
        # )
        combined_dict = {**self.data, **self._VAL_DICT}
        return combined_dict.items()

    def __missing__(self, key):
        try:
            return super().__getitem__(self._VAL_DICT[key])
        except AttributeError:
            # raise KeyError(f"Key '{key}' not found")
            raise KeyError(key)

    def __setitem__(self, key, value):
        try:
            key = self._VAL_DICT[key]
        except KeyError:
            pass
        super().__setitem__(key, value)

        # TODO: __getitem__?

    def __delitem__(self, key):
        try:
            self.data.__delitem__(key)
        except KeyError:
            # TODO: in cases we try to delete alias e.g. via pop()
            pass

        # for k, v in tuple(self._VAL_DICT.items()):
        #     if v == key:
        #         self._VAL_DICT.pop(k)
        self._VAL_DICT = {k: v for k, v in self._VAL_DICT.items() if v != key}
        # TODO: optimize

    def __contains__(self, item):
        return item in set(self.keys())

    def __iter__(self):
        for item in self.keys():
            yield item

    def __repr__(self):
        return f"AliasDict({self.items()})"

from collections import UserDict, defaultdict

from pyrio.exceptions import AliasError


class AliasDict(UserDict):
    """Custom Dict class supporting key-aliases pointing to shared values"""

    def __init__(self, dict_):
        self._alias_dict = {}
        super().__init__(self, **dict_)

    def add_alias(self, key, *aliases):
        if key not in self.data.keys():
            # raise KeyError(f"Key '{key}' not found")
            raise KeyError(key)
        for alias in aliases:
            if alias == key:
                raise AliasError(f"Key and corresponding alias cannot be equal: '{key}'")
            self._alias_dict[alias] = key

    def remove_alias(self, *aliases):
        for alias in aliases:
            self._alias_dict.__delitem__(alias)
        # TODO: use custom error -> AliasNotFound?

    def aliases(self):
        return self._alias_dict.keys()

    def aliased_keys(self):
        result = defaultdict(list)
        for alias, key in self._alias_dict.items():
            result[key].append(alias)
        return result.items()

    def origin_keys(self):
        return self.data.keys()

    def keys(self):
        return dict(**self.data, **self._alias_dict).keys()

    def values(self):
        return self.data.values()

    def items(self):
        return dict(**self.data, **self._alias_dict).items()

    def __missing__(self, key):
        try:
            return super().__getitem__(self._alias_dict[key])
        except AttributeError:
            # raise KeyError(f"Key '{key}' not found")
            raise KeyError(key)

    def __setitem__(self, key, value):
        try:
            key = self._alias_dict[key]
        except KeyError:
            pass
        super().__setitem__(key, value)

        # TODO: __getitem__?

    def __delitem__(self, key):
        try:
            self.data.__delitem__(key)
        except KeyError:
            # in case we try to delete alias e.g. via pop()
            pass
        self._alias_dict = {k: v for k, v in self._alias_dict.items() if v != key}

    def __contains__(self, item):
        return item in set(self.keys())

    def __iter__(self):
        for item in self.keys():
            yield item

    def __repr__(self):
        return f"AliasDict({self.items()})"

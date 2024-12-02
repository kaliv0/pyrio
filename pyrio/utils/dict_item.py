class DictItem:
    # TODO: add doc string
    def __init__(self, key, value):
        self._key = key
        self._value = value

    # TODO: effectively readonly properties for the user, internally use private attributes
    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._map(self._value)

    def _map(self, val):
        if isinstance(val, dict):
            return tuple(DictItem(k, self._map(v)) for k, v in val.items())
        return val

    def __repr__(self):
        return f"DictItem(key={self.key}, value={self.value})"

    def __eq__(self, other):
        return self._key == other._key and self._value == other._value  # noqa

    def __hash__(self):
        return hash((self._key, self._value))

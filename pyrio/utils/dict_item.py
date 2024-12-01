class Item:
    def __init__(self, key, value):
        self.key = key
        self.raw_value = value

    @property
    def value(self):
        return self._map(self.raw_value)

    def _map(self, val):
        if isinstance(val, dict):
            return tuple(Item(k, self._map(v)) for k, v in val.items())
        return val

    def __repr__(self):
        return f"Item(key={self.key}, value={self.value})"

    def __eq__(self, other):
        return self.key == other.key and self.value == other.value

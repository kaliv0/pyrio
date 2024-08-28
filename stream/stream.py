from stream.iterator import Iterator


class Stream:
    def __init__(self, iterable):
        self._iterable = iterable

    def __iter__(self):
        return iter(self._iterable)

    # TODO: add __eq__

    def filter(self, predicate):
        self._iterable = Iterator.filter(self._iterable, predicate)
        return self

    def map(self, mapper):
        self._iterable = Iterator.map(self._iterable, mapper)
        return self

    def flat_map(self, mapper):
        self._iterable = Iterator.flat_map(self._iterable, mapper)
        return self

    def for_each(self, function):
        return Iterator.for_each(self._iterable, function)

    def reduce(self, accumulator, identity):
        return Iterator.reduce(self._iterable, accumulator, identity)

    # ### collectors ###
    def to_list(self):
        return list(self._iterable)

    def to_tuple(self):
        return tuple(self._iterable)

    def to_set(self):
        return set(self._iterable)

    # TODO: should this be inside Iterator?
    # TODO: add logic for resolving duplicate/existing keys -> pass another lambda
    def to_dict(self, function):
        return Iterator.to_dict(self._iterable, function)

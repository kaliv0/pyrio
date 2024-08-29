from stream.iterator import Iterator


class Stream:
    def __init__(self, iterable):
        """creates Stream from a collection"""
        self._iterable = iterable

    def __iter__(self):
        return iter(self._iterable)

    def __eq__(self, other):
        # FIXME: don't use sets, (1, 2, 3) and (1, 1, 2, 3) are not equal
        return self.to_set() == other.to_set()
        # return hash(self._iterable) == hash(other._iterable)

    @staticmethod
    def of(*iterable):
        """creates Stream from args"""
        return Stream(iterable)

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

    def reduce(self, accumulator, identity=None):
        return Iterator.reduce(self._iterable, accumulator, identity)

    def distinct(self):
        self._iterable = Iterator.distinct(self._iterable)
        return self

    # ### collectors ###
    def to_list(self):
        return list(self._iterable)

    def to_tuple(self):
        return tuple(self._iterable)

    def to_set(self):
        return set(self._iterable)

    # TODO: should this be inside Iterator?
    def to_dict(self, function):
        return Iterator.to_dict(self._iterable, function)

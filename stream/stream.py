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

    def reduce(self, accumulator, identity):
        result = identity
        for i in self._iterable:
            result = accumulator(result, i)
        return result

    def to_list(self):
        return list(self._iterable)
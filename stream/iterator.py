from collections.abc import Iterable


class Iterator:
    @staticmethod
    def filter(iterable, predicate):
        for i in iterable:
            if predicate(i):
                yield i

    @staticmethod
    def map(iterable, mapper):
        for i in iterable:
            yield mapper(i)

    @staticmethod
    def filter_map(iterable, mapper):
        for i in iterable:
            if i:
                yield mapper(i)

    @staticmethod
    def flat_map(iterable, mapper):
        for i in iterable:
            # TODO: should throw if j is not iterable?
            for j in mapper(i):
                yield j

    @staticmethod
    def flatten(iterable):
        for i in iterable:
            if isinstance(i, str) or not isinstance(i, Iterable):
                yield i
            else:
                yield from Iterator.flatten(i)

    @staticmethod
    def reduce(iterable, accumulator, identity):
        # TODO: put identity as first arg- no default value?
        if len(iterable) == 0:  # TODO:?
            return identity

        idx = 0
        if identity is None:
            identity = iterable[0]
            idx = 1

        for i in iterable[idx:]:
            identity = accumulator(identity, i)
        return identity

    @staticmethod
    def for_each(iterable, function):
        for i in iterable:
            function(i)

    @staticmethod
    def iterate(seed, mapper):
        while True:
            yield seed
            seed = mapper(seed)

    @staticmethod
    def distinct(iterable):
        elements = set()
        for i in iterable:
            if i not in elements:
                elements.add(i)
                yield i

    @staticmethod
    def skip(iterable, count):
        for i in iterable:
            if count > 0:
                count -= 1
            else:
                yield i

    @staticmethod
    def limit(iterable, count):
        # TODO: check here or in stream?
        # if count < 0:
        #     raise ValueError("limit count cannot be negative")
        for i in iterable:
            if count == 0:
                break
            yield i
            count -= 1

    @staticmethod
    def sorted(iterable, key, reverse):
        # TODO: sort() or sorted()
        for i in sorted(iterable, key=key, reverse=reverse):
            yield i

    # ### ###
    @staticmethod
    def to_dict(iterable, function):
        return {k: v for k, v in (function(i) for i in iterable)}

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
    def flat_map(iterable, mapper):
        for i in iterable:
            for j in mapper(i):
                yield j

    @staticmethod
    def reduce(iterable, accumulator, identity):
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
    def distinct(iterable):
        elements = set()
        for i in iterable:
            if i not in elements:
                elements.add(i)
                yield i

    # ### ###
    @staticmethod
    def to_dict(iterable, function):
        return {k: v for k, v in (function(i) for i in iterable)}

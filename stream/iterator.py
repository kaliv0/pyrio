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
        # TODO: identity should be optional etc
        for i in iterable:
            identity = accumulator(identity, i)
        return identity

    @staticmethod
    def for_each(iterable, function):
        for i in iterable:
            function(i)

    @staticmethod
    def to_dict(iterable, function):
        # result = dict()
        # for i in iterable:
        #     k, v = function(i)
        #     result[k] = v
        # return result
        return {k: v for k, v in (function(i) for i in iterable)}

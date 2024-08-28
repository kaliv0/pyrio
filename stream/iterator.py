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
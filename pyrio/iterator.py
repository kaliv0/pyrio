class Iterator:
    @staticmethod
    def concat(*streams):
        for iterable in streams:
            for element in iterable:
                yield element

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
    def filter_map(iterable, mapper, all_falsy):
        for i in iterable:
            if (not all_falsy and i is not None) or (all_falsy and i):
                yield mapper(i)

    @staticmethod
    def flat_map(iterable, mapper):
        for i in iterable:
            # TODO: should throw if j is not iterable?
            for j in mapper(i):
                yield j

    @staticmethod
    def flatten(iterable):
        from collections.abc import Iterable

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
    def peek(iterable, function):
        for i in iterable:
            function(i)
            yield i

    @staticmethod
    def iterate(seed, mapper):
        while True:
            yield seed
            seed = mapper(seed)

    @staticmethod
    def generate(supplier):
        while True:
            yield supplier()

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
        for i in iterable:
            if count == 0:
                break
            yield i
            count -= 1

    @staticmethod
    def take_while(iterable, predicate):
        for i in iterable:
            if not predicate(i):
                break
            yield i

    @staticmethod
    def drop_while(iterable, predicate):
        # is_dropped = True
        # for i in iterable:
        #     if not predicate(i):
        #         is_dropped = False
        #     if not is_dropped:
        #         yield i

        idx = -1
        for i in iterable:
            idx += 1
            if not predicate(i):
                break

        for j in iterable[idx:]:
            yield j

    @staticmethod
    def sorted(iterable, key, reverse):
        # TODO: sort() or sorted()
        for i in sorted(iterable, key=key, reverse=reverse):
            yield i

    @staticmethod
    def compare_with(iterable, other_iterable, key):
        for i, j in zip(iterable, other_iterable):
            if (key and not key(i, j)) or i != j:
                return False
        return True

import itertools as it

from pyrio.iterator import Iterator


class ItertoolsMixin:
    def use(self, it_function, *args, **kwargs):
        import inspect

        if args:
            raise ValueError("Use keyword arguments only")

        if self._handle_no_signature_functions(it_function, **kwargs):
            return self

        signature = inspect.signature(it_function).parameters
        if self._handle_no_kwargs_functions(signature, it_function, **kwargs):
            return self

        return self._handle_default_signature_functions(signature, it_function, **kwargs)

    def _handle_no_signature_functions(self, it_function, **kwargs):
        NO_SIGNATURE_FUNCTIONS = ["chain", "islice", "product", "repeat", "zip_longest"]

        if it_function.__name__ not in NO_SIGNATURE_FUNCTIONS:
            return False

        if it_function.__name__ in ("product", "zip_longest"):
            if isinstance(self._iterable, range):
                self._iterable = it_function(self._iterable, **kwargs)
            else:
                self._iterable = it_function(*self._iterable, **kwargs)
            return True

        # functions like 'chain' don't expect key-word arguments
        self._iterable = it_function(self._iterable, *kwargs.values())
        return True

    def _handle_no_kwargs_functions(self, signature, it_function, **kwargs):
        NO_KWARGS_FUNCTIONS = ["dropwhile", "filterfalse", "starmap", "takewhile", "tee"]

        # handle functions that take only iterable as arg
        if len(signature.keys()) == 1 and "iterable" in signature:
            self._iterable = it_function(self._iterable)
            return True

        # handle functions that take no kwargs
        if it_function.__name__ in NO_KWARGS_FUNCTIONS:
            if it_function.__name__ == "tee":
                self._iterable = it_function(self._iterable, *kwargs.values())
            else:
                self._iterable = it_function(*kwargs.values(), self._iterable)
            return True
        return False

    def _handle_default_signature_functions(self, signature, it_function, **kwargs):
        if "iterable" in signature:
            kwargs["iterable"] = self._iterable
        elif "data" in signature:
            kwargs["data"] = self._iterable
        self._iterable = it_function(**kwargs)
        return self

    # ### 'recipes' ###
    @staticmethod
    def _tabulate(mapper, start=0):
        return Iterator.map(it.count(start), mapper)  # TODO: fix?

    def _repeat_func(self, operation, times=None):
        return it.starmap(operation, it.repeat(self._iterable, times=times))

    def _ncycles(self, count):
        return it.chain.from_iterable(it.repeat(tuple(self._iterable), count))

    # def _consume(self, n=None):
    #     import collections
    #
    #     if n is None:
    #         return collections.deque(self._iterable, maxlen=0)
    #     else:
    #         return next(it.islice(self._iterable, n, n), None)

    def _nth(self, n, default):
        if n < 0:
            n = len(self._iterable) + n
        return next(it.islice(self._iterable, n, None), default)

    def _all_equal(self, key=None):
        return len(list(it.islice(it.groupby(self._iterable, key), 2))) <= 1

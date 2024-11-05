from pyrio.base_stream import BaseStream
from pyrio.decorator import pre_call, handle_consumed
from pyrio.iterator import Iterator
from pyrio.itertools_mixin import ItertoolsMixin


@pre_call(handle_consumed)
class Stream(BaseStream, ItertoolsMixin):
    def __init__(self, iterable):
        """Creates Stream from a collection"""
        super().__init__(iterable)

    @classmethod
    def of(cls, *iterable):
        """Creates Stream from args"""
        return cls(iterable)

    @classmethod
    def empty(cls):
        """Creates empty Stream"""
        return cls([])

    @classmethod
    def iterate(cls, seed, operation):
        """Creates infinite ordered Stream"""
        return cls(Iterator.iterate(seed, operation))

    @classmethod
    def generate(cls, supplier):
        """Creates infinite unordered Stream with values generated by given supplier function"""
        return cls(Iterator.generate(supplier))

    @classmethod
    def constant(cls, element):
        """Creates infinite Stream with given value"""
        return cls.generate(lambda: element)

    @staticmethod
    def concat(*streams):
        """Concatenates several streams together or add new streams to the current one"""
        return Stream(Iterator.concat(*streams))

    def prepend(self, *iterable):
        """Prepends iterable to current stream"""
        self._iterable = Iterator.concat(iterable, self._iterable)
        return self

    # NB: handle_consumed decorator needs access to toggle flag
    def take_nth(self, idx, default=None):
        """Returns Optional with the nth element of the stream or a default value"""
        return super().take_nth(idx, default)

    def all_equal(self, key=None):
        """Returns True if all elements of the stream are equal to each other"""
        return super().all_equal(key)

from pyrio.streams.base_stream import BaseStream
from pyrio.iterators.iterator import Iterator
from pyrio.iterators.itertools_mixin import ItertoolsMixin
from pyrio.utils.decorator import pre_call, handle_consumed


@pre_call(handle_consumed)
class Stream(BaseStream, ItertoolsMixin):
    @classmethod
    def of(cls, *iterable):
        """Creates Stream from args"""
        return cls(iterable)

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

    # NB: handle_consumed decorator needs access to toggle flag
    def take_nth(self, idx, default=None):
        """Returns Optional with the nth element of the stream or a default value"""
        return super().take_nth(idx, default)

    def all_equal(self, key=None):
        """Returns True if all elements of the stream are equal to each other"""
        return super().all_equal(key)
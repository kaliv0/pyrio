from pyrio.decorators import pre_call, handle_consumed, dispatch
from pyrio.streams import BaseStream
from pyrio.iterators import StreamGenerator, ItertoolsMixin


@pre_call(handle_consumed)
class Stream(BaseStream, ItertoolsMixin):
    """Abstraction over a sequence of elements supporting sequential aggregate operations"""

    @classmethod
    def of(cls, *iterable):
        """Creates Stream from args"""
        if any(i is None for i in iterable):
            raise TypeError("Cannot create Stream from None")
        return cls(iterable)

    @classmethod
    def of_nullable(cls, *iterable):
        # TODO: docstr
        if any(i is None for i in iterable):
            return cls.empty()
        return cls(iterable)

    @classmethod
    def iterate(cls, seed, operation, condition=None):
        """Creates infinite ordered Stream"""
        return cls(StreamGenerator.iterate(seed, operation, condition))

    @classmethod
    def generate(cls, supplier):
        """Creates infinite unordered Stream with values generated by given supplier function"""
        return cls(StreamGenerator.generate(supplier))

    @classmethod
    def constant(cls, element):
        """Creates infinite Stream with given value"""
        return cls.generate(lambda: element)

    @classmethod
    @dispatch(int, int)
    @dispatch(int, int, int)
    def from_range(cls, start, stop, step=1):
        """Creates Stream from start (inclusive) to stop (exclusive) by an incremental step"""
        return cls(StreamGenerator.range(start, stop, step))

    @classmethod
    @dispatch(range)
    def from_range(cls, range_obj):  # noqa: F811
        """Creates Stream range object"""
        return cls(StreamGenerator.range(range_obj.start, range_obj.stop, range_obj.step))

    # NB: handle_consumed decorator needs access to toggle flag
    def take_nth(self, idx, default=None):
        """Returns Optional with the nth element of the stream or a default value"""
        return super().take_nth(idx, default)

    def all_equal(self, key=None):
        """Returns True if all elements of the stream are equal to each other"""
        return super().all_equal(key)

import io
from contextlib import redirect_stdout

from stream.stream import Stream


def test_stream():
    assert Stream([1, 2, 3])._iterable == [1, 2, 3]


def test_stream_of():
    assert Stream.of(1, 2, 3)._iterable == (1, 2, 3)


def test_filter():
    assert Stream([1, 2, 3, 4, 5, 6]).filter(lambda x: x % 2 == 0).to_list() == [2, 4, 6]


def test_map():
    # TODO: in two tests
    assert Stream([1, 2, 3]).map(str).to_list() == ["1", "2", "3"]


def test_map_lambda():
    assert Stream([1, 2, 3]).map(lambda x: x + 5).to_list() == [6, 7, 8]


def test_filter_map():
    assert Stream.of(None, "foo", "", "bar").filter_map(str.upper).to_list() == ["FOO", "BAR"]
    # TODO: should we skip zeros?
    assert Stream([0, 1, 2]).filter_map(lambda x: x + 10).to_list() == [11, 12]


def test_reduce():
    assert Stream([1, 2, 3]).reduce(lambda acc, val: acc + val, identity=3) == 9


def test_reduce_no_identity_provided():
    assert Stream([1, 2, 3]).reduce(lambda acc, val: acc + val) == 6


def test_reduce_empty_collection():
    assert Stream([]).reduce(lambda acc, val: acc + val) is None


def test_for_each():
    f = io.StringIO()
    with redirect_stdout(f):
        Stream([1, 2, 3, 4]).for_each(lambda x: print(f"{'#' * x} ", end=""))
    assert f.getvalue() == "# ## ### #### "


# ### flat ###
def test_flat_map():
    assert Stream([[1, 2], [3, 4], [5]]).flat_map(lambda x: Stream(x)).to_list() == [1, 2, 3, 4, 5]


def test_flatten():
    assert Stream([[1, 2], [3, 4], [5]]).flatten().to_list() == [1, 2, 3, 4, 5]


def test_flatten_empty():
    assert Stream([[], [1, 2]]).flatten().to_list() == [1, 2]


def test_flatten_no_nested_levels():
    assert Stream([1, 2]).flatten().to_list() == [1, 2]


def test_flatten_multiple_levels():
    assert Stream([1, 2]).flatten().to_list() == [1, 2]
    assert Stream([[[1, 2], [3, 4]], [5, 6], [7]]).flatten().to_list() == [1, 2, 3, 4, 5, 6, 7]


def test_flatten_strings():
    assert Stream([["abc"], "x", "y", "z"]).flatten().to_list() == ["abc", "x", "y", "z"]


# ### ###
def test_distinct():
    assert Stream([1, 1, 2, 2, 2, 3]).distinct().to_list() == [1, 2, 3]


def test_count():
    assert Stream([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).count() == 2


# ### ###
def test_sorted():
    assert Stream.of(3, 5, 2, 1).map(lambda x: x * 10).sorted().to_list() == [10, 20, 30, 50]


def test_sorted_reverse():
    assert Stream.of(3, 5, 2, 1).map(lambda x: x * 10).sorted(reverse=True).to_list() == [50, 30, 20, 10]


def test_sorted_comparator_function():
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(lambda x: x[1]).to_list() == [
        ("1", 10),
        ("2", 20),
        ("3", 30),
        ("5", 50),
    ]


def test_sorted_comparator_and_reverse():
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(lambda x: x[1], reverse=True).to_list() == [
        ("5", 50),
        ("3", 30),
        ("2", 20),
        ("1", 10),
    ]


def test_complex_pipeline():
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(lambda x: x[1], reverse=True).to_dict(
        lambda x: (x[0], x[1])
    ) == {"5": 50, "3": 30, "2": 20, "1": 10}


# ### ###
# TODO: ???
def test_eq():
    assert Stream.of(1, 2, 3) == Stream.of(3, 2, 1)


# ### collectors ###
def test_to_list():
    result = Stream((1, 2, 3)).to_list()
    assert result == [1, 2, 3]
    assert type(result) is list


def test_to_tuple():
    result = Stream([1, 2, 3]).to_tuple()
    assert result == (1, 2, 3)
    assert type(result) is tuple


def test_to_set():
    result = Stream([1, 2, 2, 3, 3, 3]).to_set()
    assert result == {1, 2, 3}
    assert type(result) is set


def test_to_dict():
    class Foo:
        def __init__(self, name, num):
            self.name = name
            self.num = num

    coll = [Foo("fizz", 1), Foo("buzz", 2)]
    assert Stream(coll).to_dict(lambda x: (x.name, x.num)) == {"fizz": 1, "buzz": 2}


def test_collect():
    assert Stream([1, 2, 3]).to_tuple() == (1, 2, 3)
    assert Stream.of(1, 2, 3).to_list() == [1, 2, 3]
    assert Stream.of(1, 1, 2, 2, 2, 3).collect(set) == {1, 2, 3}
    assert Stream.of(1, 2, 3, 4).collect(dict, lambda x: (str(x), x * 10)) == {"1": 10, "2": 20, "3": 30, "4": 40}

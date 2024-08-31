import io
from contextlib import redirect_stdout

import pytest

from pyrio import Stream
from pyrio.decorator import IllegalStateError


def test_stream():
    assert Stream([1, 2, 3])._iterable == [1, 2, 3]


def test_stream_of():
    assert Stream.of(1, 2, 3)._iterable == (1, 2, 3)


def test_empty_stream():
    assert Stream.empty().count() == 0


def test_iterate():
    assert Stream.iterate(0, lambda x: x + 1).limit(5).to_list() == [0, 1, 2, 3, 4]


def test_iterate_skip():
    assert Stream.iterate(0, lambda x: x + 1).skip(5).limit(5).to_list() == [5, 6, 7, 8, 9]


def test_generate():
    assert Stream.generate(lambda: 42).limit(3).to_list() == [42, 42, 42]


def test_filter():
    assert Stream([1, 2, 3, 4, 5, 6]).filter(lambda x: x % 2 == 0).to_list() == [2, 4, 6]


def test_map():
    assert Stream([1, 2, 3]).map(str).to_list() == ["1", "2", "3"]


def test_map_lambda():
    assert Stream([1, 2, 3]).map(lambda x: x + 5).to_list() == [6, 7, 8]


def test_filter_map():
    assert Stream.of(None, "foo", "bar").filter_map(str.upper).to_list() == ["FOO", "BAR"]


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


# ### skip ###
def test_skip():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).skip(7).to_tuple() == (8, 9, 10)


def test_skip_empty():
    assert Stream.empty().skip(3).to_tuple() == ()


def test_skip_bigger_than_stream_count():
    assert Stream([1, 2]).skip(5).to_tuple() == ()


# ### limit ###
def test_limit():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).limit(3).to_tuple() == (1, 2, 3)


def test_limit_empty():
    assert Stream.empty().limit(3).to_tuple() == ()


def test_limit_bigger_than_stream_count():
    assert Stream([1, 2]).limit(5).to_tuple() == (1, 2)


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
    assert Stream([[[1, 2], [3, 4]], [5, 6], [7]]).flatten().to_list() == [1, 2, 3, 4, 5, 6, 7]


def test_flatten_strings():
    assert Stream([["abc"], "x", "y", "z"]).flatten().to_list() == ["abc", "x", "y", "z"]


# ### ###
def test_distinct():
    assert Stream([1, 1, 2, 2, 2, 3]).distinct().to_list() == [1, 2, 3]


def test_count():
    assert Stream([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).count() == 2


def test_count_empty_collection():
    assert Stream([]).count() == 0
    assert Stream.empty().count() == 0


def test_sum():
    assert Stream.of(1, 2, 3, 4).sum() == 10


def test_sum_empty_collection():
    assert Stream([]).sum() == 0
    assert Stream.empty().count() == 0


def test_sum_non_number_elements():
    with pytest.raises(ValueError) as e:
        Stream.of("a", "b").sum()
    assert str(e.value) == "Cannot apply sum on non-number elements"


def test_take_while():
    assert Stream.of("adam", "aman", "ahmad", "hamid", "muhammad", "aladdin").take_while(
        lambda x: x[0] == "a"
    ).to_list() == ["adam", "aman", "ahmad"]


def test_take_while_no_elements():
    assert (
        Stream.of("adam", "aman", "ahmad", "hamid", "muhammad", "aladdin")
        .take_while(lambda x: x[0] == "xyz")
        .to_list()
        == []
    )


def test_drop_while():
    assert Stream.of("adam", "aman", "ahmad", "hamid", "muhammad", "aladdin").drop_while(
        lambda x: x[0] == "a"
    ).to_list() == ["hamid", "muhammad", "aladdin"]


def test_drop_while_no_elements():
    assert Stream.of("adam", "aman", "ahmad", "hamid", "muhammad", "aladdin").drop_while(
        lambda x: x[0] == "xyz"
    ).to_list() == ["adam", "aman", "ahmad", "hamid", "muhammad", "aladdin"]


# ### ###
def test_sorted():
    assert Stream.of(3, 5, 2, 1).map(lambda x: x * 10).sorted().to_list() == [10, 20, 30, 50]


def test_sorted_reverse():
    assert Stream.of(3, 5, 2, 1).map(lambda x: x * 10).sorted(reverse=True).to_list() == [
        50,
        30,
        20,
        10,
    ]


def test_sorted_comparator_function():
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(
        lambda x: x[1]
    ).to_list() == [
        ("1", 10),
        ("2", 20),
        ("3", 30),
        ("5", 50),
    ]


def test_sorted_comparator_and_reverse():
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(
        lambda x: x[1], reverse=True
    ).to_list() == [
        ("5", 50),
        ("3", 30),
        ("2", 20),
        ("1", 10),
    ]


def test_complex_pipeline():
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(
        lambda x: x[1], reverse=True
    ).to_dict(lambda x: (x[0], x[1])) == {"5": 50, "3": 30, "2": 20, "1": 10}


def test_reusing_stream():
    stream = Stream.of(1, 2, 3)
    assert stream._is_consumed is False

    result = stream.map(str).to_list()
    assert result == ["1", "2", "3"]
    assert stream._is_consumed is True

    with pytest.raises(IllegalStateError) as e:
        stream.map(lambda x: x * 10).to_list()
    assert str(e.value) == "Stream object already consumed"


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
    assert Stream([1, 2, 3]).collect(tuple) == (1, 2, 3)
    assert Stream.of(1, 2, 3).collect(list) == [1, 2, 3]
    assert Stream.of(1, 1, 2, 2, 2, 3).collect(set) == {1, 2, 3}
    assert Stream.of(1, 2, 3, 4).collect(dict, lambda x: (str(x), x * 10)) == {
        "1": 10,
        "2": 20,
        "3": 30,
        "4": 40,
    }

import io
from contextlib import redirect_stdout

import pytest

from pyrio import Stream
from pyrio.optional import Optional
from pyrio.exception import IllegalStateError, NoSuchElementError, NullPointerError


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


# ### find ###
def test_find_first():
    assert Stream.of(1, 2, 3, 4).filter(lambda x: x % 2 == 0).find_first().get() == 2


def test_find_first_with_predicate():
    assert Stream.of(1, 2, 3, 4).find_first(lambda x: x % 2 == 0).get() == 2


def test_find_first_in_empty_stream():
    result = Stream.empty().find_first()
    assert isinstance(result, Optional)
    assert result.is_empty() is True


def test_find_any():
    assert Stream.of(1, 2, 3, 4).filter(lambda x: x % 2 == 0).find_any().get() in (2, 4)


def test_find_any_with_predicate():
    assert Stream.of(1, 2, 3, 4).find_any(lambda x: x % 2 == 0).get() in (2, 4)


def test_find_any_in_empty_stream():
    result = Stream.empty().find_any()
    assert isinstance(result, Optional)
    assert result.is_empty() is True


# ### match ###
def test_any_match():
    assert Stream.of(1, 2, 3, 4).any_match(lambda x: x > 2) is True


def test_any_match_false():
    assert Stream.of(1, 2, 3, 4).any_match(lambda x: x > 10) is False


def test_any_match_empty():
    assert Stream.empty().any_match(lambda x: x > 10) is False


def test_all_match():
    assert Stream.of(1, 2, 3, 4).all_match(lambda x: x > 0) is True


def test_all_match_false():
    assert Stream.of(1, 2, 3, 4).all_match(lambda x: x > 10) is False


def test_all_match_empty():
    assert Stream.empty().all_match(lambda x: x > 10) is True


def test_none_match():
    assert Stream.of(1, 2, 3, 4).none_match(lambda x: x < 0) is True


def test_none_match_false():
    assert Stream.of(1, 2, 3, 4).none_match(lambda x: x < 10) is False


def test_none_match_empty():
    assert Stream.empty().none_match(lambda x: x > 10) is False


# ### min ###
def test_min():
    assert Stream.of(2, 1, 3, 4).min().get() == 1


def test_min_comparator():
    assert Stream.of("20", "101", "50").min(key=int).get() == "20"


def test_min_empty():
    result = Stream.empty().min()
    assert isinstance(result, Optional)
    assert result.is_empty() is True


def test_min_objects(Foo):
    fizz = Foo("fizz", 1)
    buzz = Foo("buzz", 2)
    coll = [fizz, buzz]
    assert Stream(coll).min(lambda x: x.num).get() is fizz


# ### max ###
def test_max():
    assert Stream.of(2, 1, 3, 4).max().get() == 4


def test_max_comparator():
    assert Stream.of("20", "101", "50").max(key=int).get() == "101"


def test_max_empty():
    result = Stream.empty().max()
    assert isinstance(result, Optional)
    assert result.is_empty() is True


def test_max_objects(Foo):
    fizz = Foo("fizz", 1)
    buzz = Foo("buzz", 2)
    coll = [fizz, buzz]
    assert Stream(coll).max(lambda x: x.num).get() is buzz


# ### optional ###
def test_optional_get_raises():
    with pytest.raises(NoSuchElementError) as e:
        Stream.empty().find_first().get()
    assert str(e.value) == "Optional is empty"


def test_optional_of_none_raises():
    with pytest.raises(NullPointerError) as e:
        Optional.of(None)
    assert str(e.value) == "Optional is empty"


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


def test_to_dict(Foo):
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

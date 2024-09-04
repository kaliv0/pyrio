import io
import itertools
import operator
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


def test_filter_map_falsy():
    assert Stream.of(None, "foo", "", "bar", 0, []).filter_map(str.upper, falsy=True).to_list() == [
        "FOO",
        "BAR",
    ]


def test_reduce():
    assert Stream([1, 2, 3]).reduce(lambda acc, val: acc + val, identity=3).get() == 9


def test_reduce_no_identity_provided():
    assert Stream([1, 2, 3]).reduce(lambda acc, val: acc + val).get() == 6


def test_reduce_empty_collection():
    assert Stream([]).reduce(lambda acc, val: acc + val).is_empty() is True


def test_for_each():
    f = io.StringIO()
    with redirect_stdout(f):
        Stream([1, 2, 3, 4]).for_each(lambda x: print(f"{'#' * x} ", end=""))
    assert f.getvalue() == "# ## ### #### "


def test_peek():
    f = io.StringIO()
    with redirect_stdout(f):
        result = (
            Stream([1, 2, 3, 4])
            .filter(lambda x: x > 2)
            .peek(lambda x: print(f"{x} ", end=""))
            .map(lambda x: x * 20)
            .to_list()
        )
    assert f.getvalue() == "3 4 "
    assert result == [60, 80]


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


def test_flat_map_raises():
    with pytest.raises(TypeError) as e:
        assert Stream([[1, 2], 3]).flat_map(lambda x: Stream(x)).to_list()
    assert str(e.value) == "'int' object is not iterable"


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


# ### sorted ###
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
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(lambda x: x[1]).to_list() == [
        ("1", 10),
        ("2", 20),
        ("3", 30),
        ("5", 50),
    ]


def test_sorted_multiple_keys():
    assert Stream.of((3, 30), (2, 30), (2, 20), (1, 20), (1, 10)).sorted(
        lambda x: (x[0], x[1])
    ).to_list() == [(1, 10), (1, 20), (2, 20), (2, 30), (3, 30)]


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
    assert Stream.of(3, 5, 2, 1).map(lambda x: (str(x), x * 10)).sorted(lambda x: x[1], reverse=True).to_dict(
        lambda x: (x[0], x[1])
    ) == {"5": 50, "3": 30, "2": 20, "1": 10}


def test_reusing_stream():
    stream = Stream.of(1, 2, 3)
    assert stream._is_consumed is False

    result = stream.map(str).to_list()
    assert result == ["1", "2", "3"]
    assert stream._is_consumed is True

    with pytest.raises(IllegalStateError) as e:
        stream.map(lambda x: x * 10).to_list()
    assert str(e.value) == "Stream object already consumed"


def test_compare_with():
    assert Stream([1, 2]).compare_with(Stream([1, 2])) is True
    assert Stream([1, 2]).compare_with(Stream([2, 1])) is False
    assert Stream([1, 2]).compare_with(Stream([3, 4])) is False


def test_compare_with_custom_key(Foo):
    fizz = Foo("fizz", 1)
    buzz = Foo("buzz", 2)
    comparator = lambda x, y: x.num == y.num  # noqa

    assert Stream([fizz, buzz]).compare_with(Stream([fizz, buzz]), comparator) is True
    assert Stream([buzz, fizz]).compare_with(Stream([fizz, buzz]), comparator) is False
    assert Stream([fizz, buzz]).compare_with(Stream([buzz]), comparator) is False


# ### concat ###
def test_concat():
    assert Stream.concat(Stream.of(1, 2, 3), Stream.of(4, 5, 6)).to_list() == [1, 2, 3, 4, 5, 6]


def test_concat_empty():
    assert Stream.concat(Stream.empty(), Stream.of(1, 2, 3)).to_list() == [1, 2, 3]
    assert Stream.concat(Stream.empty(), Stream.empty()).to_list() == []


def test_concat_collections():
    assert Stream.concat(Stream.of(1, 2, 3), [5, 6]).to_list() == [1, 2, 3, 5, 6]
    assert Stream.concat((1, 2, 3), [5, 6]).to_list() == [1, 2, 3, 5, 6]
    assert Stream.concat({1, 2, 3}, [5, 6]).to_list() == [1, 2, 3, 5, 6]

    assert Stream.concat([1, 2, 3], [(5, 6), [8]]).to_list() == [1, 2, 3, (5, 6), [8]]
    assert Stream.concat([1, 2, 3], [(5, 6), [8]]).flatten().to_list() == [1, 2, 3, 5, 6, 8]


def test_concat_raises_non_iterable():
    with pytest.raises(TypeError) as e:
        Stream.concat([1, 2, 3], 5).to_list()
    assert str(e.value) == "'int' object is not iterable"


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
    assert Stream.of("20", "101", "50").min(comparator=int).get() == "20"


def test_min_empty():
    result = Stream.empty().min()
    assert isinstance(result, Optional)
    assert result.is_empty() is True


def test_min_default_value():
    assert Stream.empty().min(default="foo").get() == "foo"
    assert Stream.empty().min(default="foo").get() == Stream.empty().min().or_else("foo")


def test_min_objects(Foo):
    fizz = Foo("fizz", 1)
    buzz = Foo("buzz", 2)
    coll = [fizz, buzz]
    assert Stream(coll).min(lambda x: x.num).get() is fizz


# ### max ###
def test_max():
    assert Stream.of(2, 1, 3, 4).max().get() == 4


def test_max_comparator():
    assert Stream.of("20", "101", "50").max(comparator=int).get() == "101"


def test_max_empty():
    result = Stream.empty().max()
    assert isinstance(result, Optional)
    assert result.is_empty() is True


def test_max_default_value():
    assert Stream.empty().max(default="foo").get() == "foo"
    assert Stream.empty().max(default="foo").get() == Stream.empty().max().or_else("foo")


def test_max_objects(Foo):
    fizz = Foo("fizz", 1)
    buzz = Foo("buzz", 2)
    coll = [fizz, buzz]
    assert Stream(coll).max(lambda x: x.num).get() is buzz


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


def test_to_dict_merger(Foo):
    coll = [Foo("fizz", 1), Foo("fizz", 2), Foo("buzz", 2)]
    assert Stream(coll).to_dict(collector=lambda x: (x.name, x.num), merger=lambda old, new: old) == {
        "fizz": 1,
        "buzz": 2,
    }


def test_to_dict_duplicate_key_no_merger_raises(Foo):
    coll = [Foo("fizz", 1), Foo("fizz", 2), Foo("buzz", 2)]
    with pytest.raises(IllegalStateError) as e:
        Stream(coll).to_dict(
            collector=lambda x: (x.name, x.num),
        )
    assert str(e.value) == "Key 'fizz' already exists"


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


def test_group_by():
    assert Stream("AAAABBBCCD").group_by() == {
        "A": ["A", "A", "A", "A"],
        "B": ["B", "B", "B"],
        "C": ["C", "C"],
        "D": ["D"],
    }


def test_group_by_custom_collector():
    assert Stream("AAAABBBCCD").group_by(collector=lambda k, g: (k, len(g))) == {
        "A": 4,
        "B": 3,
        "C": 2,
        "D": 1,
    }


def test_group_by_objects(Foo):
    coll = [
        Foo("fizz", 1),
        Foo("fizz", 2),
        Foo("fizz", 3),
        Foo("buzz", 2),
        Foo("buzz", 3),
        Foo("buzz", 4),
        Foo("buzz", 5),
    ]

    assert Stream(coll).group_by(
        classifier=lambda obj: obj.name,
        collector=lambda k, g: (k, [(obj.name, obj.num) for obj in list(g)]),
    ) == {
        "fizz": [("fizz", 1), ("fizz", 2), ("fizz", 3)],
        "buzz": [("buzz", 2), ("buzz", 3), ("buzz", 4), ("buzz", 5)],
    }


# ### itertools ###
def test_use_itertools_accumulate():
    assert Stream.of(1, 2, 3, 4, 5).use(itertools.accumulate).to_list() == list(
        itertools.accumulate([1, 2, 3, 4, 5])
    )
    assert Stream.of(1, 2, 3, 4, 5).use(itertools.accumulate, initial=100).to_list() == list(
        itertools.accumulate([1, 2, 3, 4, 5], initial=100)
    )

    # TODO: NB -> 'func' although it's 'function' in the docs (!)
    assert Stream.of(1, 2, 3, 4, 5).use(itertools.accumulate, func=operator.mul).to_list() == list(
        itertools.accumulate([1, 2, 3, 4, 5], operator.mul)
    )


def test_use_itertools_batched():
    flattened_data = ["roses", "red", "violets", "blue", "sugar", "sweet"]
    assert Stream(flattened_data).use(itertools.batched, n=2).to_list() == list(
        itertools.batched(flattened_data, 2)
    )


def test_use_itertools_chain():
    assert Stream("ABC").use(itertools.chain, iterables="DEF").to_list() == list(
        itertools.chain("ABC", "DEF")
    )


def test_use_itertools_chain_from_iterable():
    assert Stream(["ABC", "DEF"]).use(itertools.chain.from_iterable).to_list() == list(
        itertools.chain.from_iterable(["ABC", "DEF"])
    )


def test_use_itertools_combinations():
    assert Stream.of(1, 2, 3, 4).use(itertools.combinations, r=3).to_list() == list(
        itertools.combinations([1, 2, 3, 4], r=3)
    )


def test_use_itertools_combinations_with_replacement():
    assert Stream("ABC").use(itertools.combinations_with_replacement, r=2).to_list() == list(
        itertools.combinations_with_replacement("ABC", r=2)
    )


def test_use_itertools_compress():
    data = "ABCDEF"
    selectors = [1, 0, 1, 0, 1, 1]
    assert Stream(data).use(itertools.compress, selectors=selectors).to_list() == list(
        itertools.compress(data, selectors)
    )


def test_use_itertools_count():
    assert Stream.empty().use(itertools.count, start=10).limit(5).to_list() == [10, 11, 12, 13, 14]
    assert Stream.empty().use(itertools.count, start=10, step=2).limit(5).to_list() == [10, 12, 14, 16, 18]


def test_use_itertools_cycle():
    assert Stream("ABCD").use(itertools.cycle).limit(12).to_list() == [
        "A",
        "B",
        "C",
        "D",
        "A",
        "B",
        "C",
        "D",
        "A",
        "B",
        "C",
        "D",
    ]


def test_itertools_dropwhile():
    coll = [1, 4, 6, 3, 8]
    predicate = lambda x: x < 5  # noqa
    assert Stream(coll).use(itertools.dropwhile, predicate=predicate).to_list() == list(
        itertools.dropwhile(predicate, coll)
    )


def test_itertools_filterfalse():
    coll = [1, 4, 6, 3, 8]
    predicate = lambda x: x < 5  # noqa
    assert Stream(coll).use(itertools.filterfalse, predicate=predicate).to_list() == list(
        itertools.filterfalse(predicate, coll)
    )


def test_itertools_groupby():
    assert Stream("AAAABBBCCD").use(itertools.groupby).to_dict(lambda x: (x[0], list(x[1]))) == {
        "A": ["A", "A", "A", "A"],
        "B": ["B", "B", "B"],
        "C": ["C", "C"],
        "D": ["D"],
    }


def test_itertools_islice():
    letters = "ABCDEFG"
    assert Stream(letters).use(itertools.islice, stop=2).to_list() == list(itertools.islice(letters, 2))
    assert Stream(letters).use(itertools.islice, start=2, stop=None).to_list() == list(
        itertools.islice(letters, 2, None)
    )
    assert Stream(letters).use(itertools.islice, start=0, stop=None, setp=2).to_list() == list(
        itertools.islice(letters, 0, None, 2)
    )


def test_itertools_pairwise():
    letters = "ABCDEFG"
    assert Stream(letters).use(itertools.pairwise).to_list() == list(itertools.pairwise(letters))


def test_use_itertools_permutations():
    assert Stream(range(3)).use(itertools.permutations, r=3).to_list() == list(
        itertools.permutations(range(3), r=3)
    )


def test_use_itertools_product():
    assert Stream.of('ABCD', 'xy').use(itertools.product).to_list() == list(
        itertools.product('ABCD', 'xy'))

# ### optional ###
def test_optional_get_raises():
    with pytest.raises(NoSuchElementError) as e:
        Stream.empty().find_first().get()
    assert str(e.value) == "Optional is empty"


def test_optional_of_none_raises():
    with pytest.raises(NullPointerError) as e:
        Optional.of(None)
    assert str(e.value) == "Value cannot be None"


def test_print_optional():
    assert str(Stream.of(1, 2, 3, 4).filter(lambda x: x % 2 == 0).find_first()) == "Optional[2]"
    assert str(Stream.empty().filter(lambda x: x % 2 == 0).find_first()) == "Optional[None]"

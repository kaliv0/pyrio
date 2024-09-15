import itertools as it
import operator

import pytest

from pyrio import Stream


def test_accumulate():
    assert Stream.of(1, 2, 3, 4, 5).use(it.accumulate).to_list() == list(it.accumulate([1, 2, 3, 4, 5]))
    assert Stream.of(1, 2, 3, 4, 5).use(it.accumulate, initial=100).to_list() == list(
        it.accumulate([1, 2, 3, 4, 5], initial=100)
    )

    # NB -> 'func' although it's 'function' in the docs (!)
    assert Stream.of(1, 2, 3, 4, 5).use(it.accumulate, func=operator.mul).to_list() == list(
        it.accumulate([1, 2, 3, 4, 5], operator.mul)
    )


def test_batched():
    flattened_data = ["roses", "red", "violets", "blue", "sugar", "sweet"]
    assert Stream(flattened_data).use(it.batched, n=2).to_list() == list(it.batched(flattened_data, 2))


def test_chain():
    assert Stream("ABC").use(it.chain, iterables="DEF").to_list() == list(it.chain("ABC", "DEF"))


def test_chain_from_iterable():
    assert Stream(["ABC", "DEF"]).use(it.chain.from_iterable).to_list() == list(
        it.chain.from_iterable(["ABC", "DEF"])
    )


def test_combinations():
    assert Stream.of(1, 2, 3, 4).use(it.combinations, r=3).to_list() == list(
        it.combinations([1, 2, 3, 4], r=3)
    )


def test_combinations_with_replacement():
    assert Stream("ABC").use(it.combinations_with_replacement, r=2).to_list() == list(
        it.combinations_with_replacement("ABC", r=2)
    )


def test_compress():
    data = "ABCDEF"
    selectors = [1, 0, 1, 0, 1, 1]
    assert Stream(data).use(it.compress, selectors=selectors).to_list() == list(it.compress(data, selectors))


def test_count():
    assert Stream.empty().use(it.count, start=10).limit(5).to_list() == [10, 11, 12, 13, 14]
    assert Stream.empty().use(it.count, start=10, step=2).limit(5).to_list() == [10, 12, 14, 16, 18]


def test_cycle():
    assert Stream("ABCD").use(it.cycle).limit(12).to_list() == [
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
    assert Stream(coll).use(it.dropwhile, predicate=predicate).to_list() == list(
        it.dropwhile(predicate, coll)
    )


def test_itertools_filterfalse():
    coll = [1, 4, 6, 3, 8]
    predicate = lambda x: x < 5  # noqa
    assert Stream(coll).use(it.filterfalse, predicate=predicate).to_list() == list(
        it.filterfalse(predicate, coll)
    )


def test_itertools_groupby():
    assert Stream("AAAABBBCCD").use(it.groupby).to_dict(lambda x: (x[0], list(x[1]))) == {
        "A": ["A", "A", "A", "A"],
        "B": ["B", "B", "B"],
        "C": ["C", "C"],
        "D": ["D"],
    }


def test_itertools_islice():
    letters = "ABCDEFG"
    assert Stream(letters).use(it.islice, stop=2).to_list() == list(it.islice(letters, 2))
    assert Stream(letters).use(it.islice, start=2, stop=None).to_list() == list(it.islice(letters, 2, None))
    assert Stream(letters).use(it.islice, start=0, stop=None, setp=2).to_list() == list(
        it.islice(letters, 0, None, 2)
    )


def test_itertools_pairwise():
    letters = "ABCDEFG"
    assert Stream(letters).use(it.pairwise).to_list() == list(it.pairwise(letters))


def test_permutations():
    assert Stream(range(3)).use(it.permutations, r=3).to_list() == list(it.permutations(range(3), r=3))


def test_product():
    assert Stream.of("ABCD", "xy").use(it.product).to_list() == list(it.product("ABCD", "xy"))
    assert Stream.of([1, 2, 3, 4], [5, 6]).use(it.product).to_list() == list(it.product([1, 2, 3, 4], [5, 6]))
    assert Stream(range(3)).use(it.product, repeat=2).to_list() == list(it.product(range(3), repeat=2))


def test_repeat():
    assert Stream(10).use(it.repeat).limit(3).to_list() == [10, 10, 10]
    assert Stream(10).use(it.repeat, times=3).to_list() == list(it.repeat(10, times=3))


def test_starmap():
    assert Stream([(2, 5), (3, 2), (10, 3)]).use(it.starmap, function=pow).to_list() == list(
        it.starmap(pow, [(2, 5), (3, 2), (10, 3)])
    )


def test_itertools_takewhile():
    coll = [1, 4, 6, 3, 8]
    predicate = lambda x: x < 5  # noqa
    assert Stream(coll).use(it.takewhile, predicate=predicate).to_list() == list(
        it.takewhile(predicate, coll)
    )


def test_tee():
    coll = [1, 2, 3, 4, 5, 6]
    assert Stream(coll).use(it.tee, n=2).map(tuple).to_list() == [tuple(s) for s in it.tee(coll, 2)]


def test_zip_longest():
    assert Stream.of("ABCD", "xy").use(it.zip_longest, fillvalue="-").to_list() == list(
        it.zip_longest("ABCD", "xy", fillvalue="-")
    )
    assert Stream.of(range(3), range(2)).use(it.zip_longest).to_list() == list(
        it.zip_longest(range(3), range(2))
    )


# ### itertools  'recipes' ###
def test_tabulate():
    assert Stream.empty().tabulate(lambda x: x**2).limit(3).to_list() == [0, 1, 4]
    assert Stream.empty().tabulate(lambda x: x**2, start=3).limit(3).to_list() == [9, 16, 25]


def test_repeat_func():
    operation = lambda x, y: x * y  # noqa
    args = [2, 3]
    times = 4
    assert Stream(args).repeat_func(operation=operation, times=times).to_list() == [6, 6, 6, 6]


def test_ncycles():
    coll = {1, 2, 3}
    count = 2
    assert Stream(coll).ncycles(count).to_list() == [1, 2, 3, 1, 2, 3]


def test_ncycles_zero_times():
    assert Stream({1, 2, 3}).ncycles(count=0).to_list() == []


def test_ncycles_negative_times():
    assert Stream({1, 2, 3}).ncycles(count=-2).to_list() == []


def test_consume():
    assert Stream.of(2, 3, 4, 5).consume(n=2).to_list() == [4, 5]


def test_consume_default_start():
    assert Stream.of(2, 3, 4, 5).consume().to_list() == []


def test_consume_negative_start():
    with pytest.raises(ValueError) as e:
        Stream.of(2, 3, 4, 5).consume(n=-2).to_list()
    assert str(e.value) == "Consume boundary cannot be negative"


def test_nth():
    assert Stream.of(2, 3, 4).nth(1) == 3


def test_nth_default_value():
    assert Stream.of(2, 3, 4).nth(10, default=66) == 66


def test_nth_negative_index():
    assert Stream.of(2, 3, 4).nth(-1) == 4


def test_all_equal():
    assert Stream([2, 2, 2]).all_equal(key=int)


def test_all_equal_false():
    assert Stream([2, 5, 3]).all_equal() is False


def test_all_equal_custom_key(Foo):
    fizz = Foo("fizz", 42)
    buzz = Foo("buzz", 42)
    coll = [fizz, buzz]
    assert Stream(coll).all_equal(key=lambda x: x.num)
    assert Stream(coll).all_equal(key=lambda x: x.name) is False


# ### view ###
def test_view():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(2, 6).to_list() == [3, 4, 5, 6]


def test_view_default_stop():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(4).to_list() == [5, 6, 7, 8, 9]


def test_view_default_boundaries():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view().to_list() == [1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_view_custom_step():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(step=2).to_list() == [1, 3, 5, 7, 9]


def test_view_custom_stop():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(stop=-3).to_list() == [1, 2, 3, 4, 5, 6]


def test_view_negative_start():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(-3).to_list() == [7, 8, 9]


def test_view_negative_stop():
    assert Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(stop=-4).to_list() == [1, 2, 3, 4, 5]


def test_view_custom_boundaries():
    coll = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert Stream(coll).view(2, -3).to_list() == [3, 4, 5, 6]
    assert Stream(coll).view(-5, -2).to_list() == [5, 6, 7]


def rest_view_negative_step():
    with pytest.raises(ValueError) as e:
        Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(step=-1).to_list()
    assert str(e.value) == "Step must be a positive integer or None"


# ### sliding window ###
def test_sliding_window():
    assert Stream("ABCDEFG").sliding_window(4).to_list() == [
        ("A", "B", "C", "D"),
        ("B", "C", "D", "E"),
        ("C", "D", "E", "F"),
        ("D", "E", "F", "G"),
    ]


def test_sliding_window_empty_collection():
    assert Stream.empty().sliding_window(2).to_list() == []


def test_sliding_window_negative_count():
    with pytest.raises(ValueError) as e:
        Stream("ABCD").sliding_window(-1).to_list()
    assert str(e.value) == "Window size cannot be negative"


def test_subslices():
    assert Stream("ABCD").subslices().to_list() == [
        "A",
        "AB",
        "ABC",
        "ABCD",
        "B",
        "BC",
        "BCD",
        "C",
        "CD",
        "D",
    ]


def test_subslices_empty_collection():
    assert Stream.empty().subslices().to_list() == []

import itertools
import operator

from pyrio import Stream


def test_accumulate():
    assert Stream.of(1, 2, 3, 4, 5).use(itertools.accumulate).to_list() == list(
        itertools.accumulate([1, 2, 3, 4, 5])
    )
    assert Stream.of(1, 2, 3, 4, 5).use(itertools.accumulate, initial=100).to_list() == list(
        itertools.accumulate([1, 2, 3, 4, 5], initial=100)
    )

    # NB -> 'func' although it's 'function' in the docs (!)
    assert Stream.of(1, 2, 3, 4, 5).use(itertools.accumulate, func=operator.mul).to_list() == list(
        itertools.accumulate([1, 2, 3, 4, 5], operator.mul)
    )


def test_batched():
    flattened_data = ["roses", "red", "violets", "blue", "sugar", "sweet"]
    assert Stream(flattened_data).use(itertools.batched, n=2).to_list() == list(
        itertools.batched(flattened_data, 2)
    )


def test_chain():
    assert Stream("ABC").use(itertools.chain, iterables="DEF").to_list() == list(
        itertools.chain("ABC", "DEF")
    )


def test_chain_from_iterable():
    assert Stream(["ABC", "DEF"]).use(itertools.chain.from_iterable).to_list() == list(
        itertools.chain.from_iterable(["ABC", "DEF"])
    )


def test_combinations():
    assert Stream.of(1, 2, 3, 4).use(itertools.combinations, r=3).to_list() == list(
        itertools.combinations([1, 2, 3, 4], r=3)
    )


def test_combinations_with_replacement():
    assert Stream("ABC").use(itertools.combinations_with_replacement, r=2).to_list() == list(
        itertools.combinations_with_replacement("ABC", r=2)
    )


def test_compress():
    data = "ABCDEF"
    selectors = [1, 0, 1, 0, 1, 1]
    assert Stream(data).use(itertools.compress, selectors=selectors).to_list() == list(
        itertools.compress(data, selectors)
    )


def test_count():
    assert Stream.empty().use(itertools.count, start=10).limit(5).to_list() == [10, 11, 12, 13, 14]
    assert Stream.empty().use(itertools.count, start=10, step=2).limit(5).to_list() == [10, 12, 14, 16, 18]


def test_cycle():
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


def test_permutations():
    assert Stream(range(3)).use(itertools.permutations, r=3).to_list() == list(
        itertools.permutations(range(3), r=3)
    )


def test_product():
    assert Stream.of("ABCD", "xy").use(itertools.product).to_list() == list(itertools.product("ABCD", "xy"))
    assert Stream.of([1, 2, 3, 4], [5, 6]).use(itertools.product).to_list() == list(
        itertools.product([1, 2, 3, 4], [5, 6])
    )
    assert Stream(range(3)).use(itertools.product, repeat=2).to_list() == list(
        itertools.product(range(3), repeat=2)
    )


def test_repeat():
    assert Stream(10).use(itertools.repeat).limit(3).to_list() == [10, 10, 10]
    assert Stream(10).use(itertools.repeat, times=3).to_list() == list(itertools.repeat(10, times=3))


def test_starmap():
    assert Stream([(2, 5), (3, 2), (10, 3)]).use(itertools.starmap, function=pow).to_list() == list(
        itertools.starmap(pow, [(2, 5), (3, 2), (10, 3)])
    )


def test_itertools_takewhile():
    coll = [1, 4, 6, 3, 8]
    predicate = lambda x: x < 5  # noqa
    assert Stream(coll).use(itertools.takewhile, predicate=predicate).to_list() == list(
        itertools.takewhile(predicate, coll)
    )


def test_tee():
    coll = [1, 2, 3, 4, 5, 6]
    assert Stream(coll).use(itertools.tee, n=2).map(tuple).to_list() == [
        tuple(s) for s in itertools.tee(coll, 2)
    ]


def test_zip_longest():
    assert Stream.of("ABCD", "xy").use(itertools.zip_longest, fillvalue="-").to_list() == list(
        itertools.zip_longest("ABCD", "xy", fillvalue="-")
    )
    assert Stream.of(range(3), range(2)).use(itertools.zip_longest).to_list() == list(
        itertools.zip_longest(range(3), range(2))
    )


# ### itertools  'recipes' ###
def test_tabulate():
    assert Stream.empty().tabulate(lambda x: x**2).limit(3).to_list() == [0, 1, 4]
    assert Stream.empty().tabulate(lambda x: x**2, start=3).limit(3).to_list() == [9, 16, 25]

<p align="center">
  <img src="https://github.com/kaliv0/pyrio/blob/main/assets/Pyrio.jpg?raw=true" width="400" alt="Pyrio">
</p>

# PYRIO


![Python 3.x](https://img.shields.io/badge/python-3.12-blue?style=flat-square&logo=Python&logoColor=white)
[![tests](https://img.shields.io/github/actions/workflow/status/kaliv0/pyrio/ci.yml)](https://github.com/kaliv0/pyrio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kaliv0/pyrio/graph/badge.svg?token=7EEG43BL33)](https://codecov.io/gh/kaliv0/pyrio)
[![docs](https://readthedocs.org/projects/pyrio/badge/?version=latest)](https://pyrio.readthedocs.io/en/latest)
[![PyPI](https://img.shields.io/pypi/v/pyrio.svg)](https://pypi.org/project/pyrio/)
[![Downloads](https://static.pepy.tech/badge/pyrio)](https://pepy.tech/projects/pyrio)

<br><b>Functional-style Streams API library</b><br>
<br>Facilitates processing of collections and iterables using fluent APIs.
<br>Gives access to files of various types (<i>json</i>, <i>toml</i>, <i>yaml</i>, <i>xml</i>, <i>csv</i>, <i>tsv</i>, <i>plain text</i>) for reading and executing complex queries.
<br>Provides easy integration with <i>itertools</i>.
<br>(NB: Commonly used <i>itertools 'recipes'</i> are included as part of the main APIs.)

## How to use
### Creating streams
- stream from iterable
```python
Stream([1, 2, 3])
```

- from variadic arguments
```python
Stream.of(1, 2, 3)
```

- empty stream
```python
Stream.empty()
```

- infinite ordered stream
```python
Stream.iterate(0, lambda x: x + 1)
```

NB: in similar fashion you can create <i>finite ordered stream</i> by providing a <i>condition</i> predicate</i>
```python
Stream.iterate(10, operation=lambda x: x + 1, condition=lambda x: x < 15).to_list()
# [10, 11, 12, 13, 14]
```

- infinite unordered stream
```python
import random

Stream.generate(lambda: random.random())
```

- infinite stream with given value
```python
Stream.constant(42)
```

- stream from range
<br>(from <i>start</i> (inclusive) to <i>stop</i> (exclusive) by an incremental <i>step</i> (defaults to 1))
```python
Stream.from_range(0, 10).to_list()
Stream.from_range(0, 10, 3).to_list()
Stream.from_range(10, -1, -2).to_list()
```
(or from <i>range</i> object)
```python
range_obj = range(0, 10)
Stream.from_range(range_obj).to_list()
```

- concat
<br>(concatenate new streams/iterables with the current one)
```python
Stream.of(1, 2, 3).concat(Stream.of(4, 5)).to_list()
Stream([1, 2, 3]).concat([5, 6]).to_list()
```

- prepend
<br>(prepend new stream/iterable to the current one)
```python
Stream([2, 3, 4]).prepend(0, 1).to_list()
Stream.of(3, 4, 5).prepend(Stream.of([0, 1], 2)).to_list()
```

NB: creating new stream from None raises error.
<br>In cases when the <i>iterable</i> could potentially be None use the <i>of_nullable()</i> method instead;
<br>it returns an <i>empty stream</i> if None and a <i>regular</i> one otherwise

--------------------------------------------
### Intermediate operations
- filter
```python
Stream([1, 2, 3]).filter(lambda x: x % 2 == 0)
```

- map
```python
Stream([1, 2, 3]).map(str).to_list()
Stream([1, 2, 3]).map(lambda x: x + 5).to_list()
```

- filter_map
<br>(filter out all None or discard_falsy values (if discard_falsy=True) and applies mapper function to the elements of the stream)
```python
Stream.of(None, "foo", "", "bar", 0, []).filter_map(str.upper, discard_falsy=True).to_list()
# ["FOO", "BAR"]
```

- flat_map
<br>(map each element of the stream and yields the elements of the produced iterators)
```python
Stream([[1, 2], [3, 4], [5]]).flat_map(lambda x: Stream(x)).to_list()
# [1, 2, 3, 4, 5]
```

- flatten
```python
Stream([[1, 2], [3, 4], [5]]).flatten().to_list()
# [1, 2, 3, 4, 5]
```

- reduce
<br>(returns Optional)
```python
Stream([1, 2, 3]).reduce(lambda acc, val: acc + val, identity=3).get()
```

- peek
<br>(perform the provided operation on each element of the stream without consuming it)
```python
(Stream([1, 2, 3, 4])
    .filter(lambda x: x > 2)
    .peek(lambda x: print(f"{x} ", end=""))
    .map(lambda x: x * 20)
    .to_list())
```

- enumerate
<br>(returns each element of the Stream preceded by his corresponding index
(by default starting from 0 if not specified otherwise))
```python
iterable = ["x", "y", "z"]
Stream(iterable).enumerate().to_list()
Stream(iterable).enumerate(start=1).to_list()
# [(0, "x"), (1, "y"), (2, "z")]
# [(1, "x"), (2, "y"), (3, "z")]
```

- view
<br>(provides access to a selected part of the stream)
```python
Stream([1, 2, 3, 4, 5, 6, 7, 8, 9]).view(start=1, stop=-3, step=2).to_list()
# [2, 4, 6]
```

- distinct
<br>(returns a stream with the distinct elements of the current one)
```python
Stream([1, 1, 2, 2, 2, 3]).distinct().to_list()
```

- skip
<br>(discards the first n elements of the stream and returns a new stream with the remaining ones)
```python
Stream.iterate(0, lambda x: x + 1).skip(5).limit(5).to_list()
```

- limit / head
<br>(returns a stream with the first n elements, or fewer if the underlying iterator ends sooner)
```python
Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).limit(3).to_tuple()
Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).head(3).to_tuple()
```

- tail
<br>(returns a stream with the last n elements, or fewer if the underlying iterator ends sooner)
```python
Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).tail(3).to_tuple()
```

- take_while
<br>(returns a stream that yields elements based on a predicate)
```python
Stream.of(1, 2, 3, 4, 5, 6, 7, 2, 3).take_while(lambda x: x < 5).to_list()
# [1, 2, 3, 4]
```

- drop_while
<br>(returns a stream that skips elements based on a predicate and yields the remaining ones)
```python
Stream.of(1, 2, 3, 5, 6, 7, 2).drop_while(lambda x: x < 5).to_list()
# [5, 6, 7, 2]
```

- sort
<br>(sorts the elements of the current stream according to natural order or based on the given comparator;
<br>if 'reverse' flag is True, the elements are sorted in descending order)
```python
(Stream.of((3, 30), (2, 30), (2, 20), (1, 20), (1, 10))
    .sort(lambda x: (x[0], x[1]), reverse=True)
    .to_list())
# [(3, 30), (2, 30), (2, 20), (1, 20), (1, 10)]
```

- reverse
<br>(sorts the elements of the current stream in reverse order;
<br>alias for <i>'sort(collector, reverse=True)'</i>)
```python
(Stream.of((3, 30), (2, 30), (2, 20), (1, 20), (1, 10))
    .reverse(lambda x: (x[0], x[1]))
    .to_list())
# [(3, 30), (2, 30), (2, 20), (1, 20), (1, 10)]
```

<br>NB: in case of stream of dicts all key-value pairs are represented internally as <i>DictItem</i> objects
<br>(including recursively for nested Mapping structures)
<br>to provide more convenient intermediate operations syntax e.g.
```python
first_dict = {"a": 1, "b": 2}
second_dict = {"x": 3, "y": 4}
(Stream(first_dict).concat(second_dict)
    .filter(lambda x: x.value % 2 == 0)
    .map(lambda x: x.key)
    .to_list())
```

- on_close
<br>(returns an equivalent Stream with an additional <i>close handler</i> to be invoked automatically by the <i>terminal operation</i>)
```python
(Stream([1, 2, 3, 4])
    .on_close(lambda: print("Sorry Montessori"))
    .peek(lambda x: print(f"{'$' * x} ", end=""))
    .map(lambda x: x * 2)
    .to_list())
# "$ $$ $$$ $$$$ Sorry Montessori"
# [2, 4, 6, 8]
```
--------------------------------------------
### Terminal operations
#### Collectors
- collecting result into list, tuple, set
```python
Stream([1, 2, 3]).to_list()
Stream([1, 2, 3]).to_tuple()
Stream([1, 2, 3]).to_set()
```

- into dict
```python
class Foo:
    def __init__(self, name, num):
        self.name = name
        self.num = num

Stream([Foo("fizz", 1), Foo("buzz", 2)]).to_dict(lambda x: (x.name, x.num))
# {"fizz": 1, "buzz": 2}
```

In the case of a collision (duplicate keys) the 'merger' functions indicates which entry should be kept
```python
collection = [Foo("fizz", 1), Foo("fizz", 2), Foo("buzz", 2)]
Stream(collection).to_dict(collector=lambda x: (x.name, x.num), merger=lambda old, new: old)
# {"fizz": 1, "buzz": 2}
```

<i>to_dict</i> method also supports creating dictionaries from dict DictItem objects
```python
first_dict = {"x": 1, "y": 2}
second_dict = {"p": 33, "q": 44, "r": None}
Stream(first_dict).concat(Stream(second_dict)).to_dict(lambda x: DictItem(x.key, x.value or 0))
# {"x": 1, "y": 2, "p": 33, "q": 44, "r": 0}
```
e.g. you could combine streams of dicts by writing:
```python
Stream(first_dict).concat(Stream(second_dict)).to_dict()
```
(simplified from <i>'.to_dict(lambda x: x)'</i>)

- into string
```python
Stream({"a": 1, "b": [2, 3]}).to_string()
# "Stream(DictItem(key=a, value=1), DictItem(key=b, value=[2, 3]))"
```
```python
Stream({"a": 1, "b": [2, 3]}).map(lambda x: {x.key: x.value}).to_string(delimiter=" | ")
# "Stream({'a': 1} | {'b': [2, 3]})"
```

- alternative for working with collectors is using the <i>collect</i> method
```python
Stream([1, 2, 3]).collect(tuple)
Stream.of(1, 2, 3).collect(list)
Stream.of(1, 1, 2, 2, 2, 3).collect(set)
Stream.of(1, 2, 3, 4).collect(dict, lambda x: (str(x), x * 10))
Stream.of("x", "y", "z").collect(str, str_delimiter="->")
```

- grouping
```python
Stream("AAAABBBCCD").group_by(collector=lambda key, grouper: (key, len(grouper)))
# {"A": 4, "B": 3, "C": 2, "D": 1}
```

```python
coll = [Foo("fizz", 1), Foo("fizz", 2), Foo("fizz", 3), Foo("buzz", 2), Foo("buzz", 3), Foo("buzz", 4), Foo("buzz", 5)]
Stream(coll).group_by(
    classifier=lambda obj: obj.name,
    collector=lambda key, grouper: (key, [(obj.name, obj.num) for obj in list(grouper)]))
# {"fizz": [("fizz", 1), ("fizz", 2), ("fizz", 3)],
#  "buzz": [("buzz", 2), ("buzz", 3), ("buzz", 4), ("buzz", 5)]}
```
#### Other terminal operations
- for_each
```python
Stream([1, 2, 3, 4]).for_each(lambda x: print(f"{'#' * x} ", end=""))
```

- count
<br>(returns the count of elements in the stream)
```python
Stream([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).count()
```

- sum
```python
Stream.of(1, 2, 3, 4).sum()
```

- min
<br>(returns Optional with the minimum element of the stream)
```python
Stream.of(2, 1, 3, 4).min().get()
```

- max
<br>(returns Optional with the maximum element of the stream)
```python
Stream.of(2, 1, 3, 4).max().get()
```

- average
<br>(returns the average value of elements in the stream)
```python
Stream.of(1, 2, 3, 4, 5).average()
```

- find_first
<br>(search for an element of the stream that satisfies a predicate,
returns an Optional with the first found value, if any, or None)
```python
Stream.of(1, 2, 3, 4).filter(lambda x: x % 2 == 0).find_first().get()
```

- find_any
<br>(search for an element of the stream that satisfies a predicate,
returns an Optional with some of the found values, if any, or None)
```python
Stream.of(1, 2, 3, 4).filter(lambda x: x % 2 == 0).find_any().get()
```

- any_match
<br>(returns whether any elements of the stream match the given predicate)
```python
Stream.of(1, 2, 3, 4).any_match(lambda x: x > 2)
```

- all_match
<br>(returns whether all elements of the stream match the given predicate)
```python
Stream.of(1, 2, 3, 4).all_match(lambda x: x > 2)
```

- none_match
<br>(returns whether no elements of the stream match the given predicate)
```python
Stream.of(1, 2, 3, 4).none_match(lambda x: x < 0)
```

- take_first
<br>(returns Optional with the first element of the stream or a default value)
```python
Stream({"a": 1, "b": 2}).take_first().get()
Stream([]).take_first(default=33).get()
# DictItem(key="a", value=1)
# 33
```

- take_last
<br>(returns Optional with the last element of the stream or a default value)
```python
Stream({"a": 1, "b": 2}).take_last().get()
Stream([]).take_last(default=33).get()
```

- compare_with
<br>(compares linearly the contents of two streams based on a given comparator)
```python
fizz = Foo("fizz", 1)
buzz = Foo("buzz", 2)
Stream([buzz, fizz]).compare_with(Stream([fizz, buzz]), lambda x, y: x.num == y.num)
```

- quantify
<br>(count how many of the elements are Truthy or evaluate to True based on a given predicate)
```python
Stream([2, 3, 4, 5, 6]).quantify(predicate=lambda x: x % 2 == 0)
```

NB: although the Stream is closed automatically by the <i>terminal operation</i>
<br> you can still close it by hand (if needed) invoking the <i>close()</i> method.
<br> In turn that will trigger the <i>close_handler</i> (if such was provided)

--------------------------------------------
### Itertools integration
Invoke <i>use</i> method by passing the itertools function and it's arguments as **kwargs
```python
import itertools
import operator

Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).use(itertools.islice, start=3, stop=8)
Stream.of(1, 2, 3, 4, 5).use(itertools.accumulate, func=operator.mul).to_list()
Stream(range(3)).use(itertools.permutations, r=3).to_list()

```
#### Itertools 'recipes'
Invoke the 'recipes' described [here](https://docs.python.org/3/library/itertools.html#itertools-recipes) as stream methods and pass required key-word arguments
```python
Stream([1, 2, 3]).ncycles(count=2).to_list()
Stream.of(2, 3, 4).take_nth(10, default=66).get()
Stream(["ABC", "D", "EF"]).round_robin().to_list()
```

--------------------------------------------
### FileStreams
#### Querying files
- working with <i>json</i>, <i>toml</i>, <i>yaml</i>, <i>xml</i> files
<br>NB: FileStream reads data as series of DictItem objects from underlying dict_items view
```python
FileStream("path/to/file").map(lambda x: f"{x.key}=>{x.value}").to_tuple()
# ("abc=>xyz", "qwerty=>42")
```

```python
from operator import attrgetter
from pyrio import DictItem

(FileStream("path/to/file")
 .filter(lambda x: "a" in x.key)
 .map(lambda x: DictItem(x.key, sum(x.value) * 10))
 .sort(attrgetter("value"), reverse=True)
 .map(lambda x: f"{str(x.value)}::{x.key}")
 .to_list())
# ["230::xza", "110::abba", "30::a"]
```

- querying <i>csv</i> and <i>tsv</i> files
<br>(each row is read as a dict with keys taken from the header)
```python
FileStream("path/to/file").map(lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}").to_tuple()
# ("fizz: 42, buzz: 45", "fizz: aaa, buzz: bbb")
```
```python
from operator import itemgetter

FileStream("path/to/file").map(itemgetter('fizz')).to_list()
# ['42', 'aaa']
```
You could query the nested dicts by creating streams out of them
```python
(FileStream("path/to/file")
    .map(lambda x: (Stream(x).to_dict(lambda y: DictItem(y.key, y.value or "Unknown"))))
    .save())
```

- reading <i>plain text</i> (if the file doesn't have one of the aforementioned extensions)
```python
(FileStream("path/to/lorem/ipsum")
    .map(lambda x: x.strip())
    .enumerate()
    .filter(lambda line: "id" in line[1])
    .to_dict()
)

# {1: "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
#  6: "Excepteur sint occaecat cupidatat non proident, sunt in culpa",
#  7: "qui officia deserunt mollit anim id est laborum."}
```

- reading a file with <i>process()</i> method
  - use extra <i>f_open_options</i> (for the underlying <i>open file</i> function)
  - <i>f_read_options</i> (to be passed to the corresponding library function that is loading the file content e.g. tomllib, json)
```python
from decimal import Decimal

(FileStream.process(
    file_path="path/to/file.json",
    f_open_options={"encoding": "utf-8"},
    f_read_options={"parse_float": Decimal})
 .map(lambda x:x.value).to_list())
# ['foo', True, Decimal('1.22'), Decimal('5.456367654)]
```
To include the <i>root</i> tag when loading an <i>.xml</i> file pass <i>'include_root=True'</i>
```python
FileStream.process("path/to/custom_root.xml", include_root=True).map(
    lambda x: f"root={x.key}: inner_records={str(x.value)}"
).to_list()
# ["root=custom-root: inner_records={'abc': 'xyz', 'qwerty': '42'}"]
```

--------------------------------------------
#### Saving to a file
- write the contents of a FileStream by passing a <i>file_path</i> to the <i>save()</i> method
```python
in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
FileStream("path/to/file.json").prepend(in_memory_dict).save("./tests/resources/updated.json")
```
If no path is given, the source file for the FileStream will be <i>updated</i>
```python
FileStream("path/to/file.json").concat(in_memory_dict).save()
```
NB: if while updating the file something goes wrong, the original content will be restored/preserved

- handle null values
<br>(pass <i>null_handler</i> function to replace null values)
```python
FileStream("path/to/test.toml").save(null_handler=lambda x: DictItem(x.key, x.value or "N/A"))
```
NB: useful for writing <i>.toml</i> files which don't allow None values

- passing advanced <i>file open</i> and <i>write</i> options
<br>similarly to the <i>process</i> method you could provide
  - <i>f_open_options</i> (for the underlying <i>open</i> function)
  - <i>f_write_options</i> (passed to the corresponding library that will 'dump' the contents of the stream e.g. tomli-w, pyyaml)
```python
FileStream("path/to/file.json").concat(in_memory_dict).save(
    file_path="merged.xml",
    f_open_options={"encoding": "utf-8"},
    f_write_options={"indent": 4},
)
```
E.g. to <i>append</i> to existing file pass <i>f_open_options={"mode": "a"}</i> to the <i>save()</i> method.
<br>NB: By default saving <i>plain text</i> uses <i>"\n"</i> as <i>delimiter</i> between items,
<br>you can pass <i>custom delimiter</i> using <i>f_write_options</i>
```python
(FileStream("path/to/lorem/ipsum")
    .map(lambda line: line.strip())
    .enumerate()
    .filter(lambda line: "ad" in line[1])
    .map(lambda line: f"line:{line[0]}, text='{line[1]}'")
    .save(f_open_options={"mode": "a"}, f_write_options={"delimiter": " || "})
)

# Lorem ipsum...
# ...
# line:0, text='Lorem ipsum dolor sit amet, consectetur adipisicing elit,' || line:2, text='Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris'
```
When working with <i>plain text</i> you can pass <i>'header'</i> and <i>'footer'</i> as <i>f_write_options</i>
<br>to be prepended or appended to the FileStream output
```python
(FileStream("path/to/lorem/ipsum")
  .map(lambda line: line.strip())
  .enumerate()
  .filter(lambda line: line[0] == 3)
  .map(lambda line: f"{line[0]}: {line[1]}")
  .save(f_open_options={"mode": "a"}, f_write_options={"header": "\nHeader\n", "footer": "\nFooter\n"})
 )

# Lorem ipsum...
# ...
# qui officia deserunt mollit anim id est laborum.
#
# Header
# 3: nisi ut aliquip ex ea commodo consequat.
# Footer
#
```

To add <i>custom root</i> tag when saving an <i>.xml</i> file pass <i>'xml_root="my-custom-root"'</i>
```python
FileStream("path/to/file.json").concat(in_memory_dict).save(
    file_path="path/to/custom.xml",
    f_open_options={"encoding": "utf-8"},
    f_write_options={"indent": 4},
    xml_root="my-custom-root",
)
```

--------------------------------------------
### How far can we actually push it?
```python
(
    FileStream("path/to/file.csv")
    .concat(
        FileStream("path/to/other/file.json")
        .filter(
            lambda x: (
                Stream(x.value)
                .find_first(lambda y: y.key == "name" and y.value != "Snake")
                .or_else_get(lambda: None)
            )
            is not None
        )
        .map(lambda x: x.value)
    )
    .map(lambda x: (Stream(x).to_dict(lambda y: DictItem(y.key, y.value or "N/A"))))
    .save("path/to/third/file.tsv")
)
```
- ...some leetcode maybe?
```python
#  check if given string is palindrome; string length is guaranteed to be > 0
def validate_str(string):
    stop = len(string) // 2 if len(string) > 1 else 1
    return Stream.from_range(0, stop).none_match(lambda x: string[x] != string[x - 1])

validate_str("a1b2c3c2b1a")
validate_str("abc321")
validate_str("x")

# True
# False
# True
```
- ...and another one?
```python
# count vowels and constants in given string
def process_str(string):
    ALL_VOWELS = "AEIOUaeiou"
    return (Stream(string)
        .filter(lambda ch: ch.isalpha())
        .partition(lambda ch: ch in ALL_VOWELS)  # Partitions entries into true and false ones
        .map(lambda p: tuple(p))
        .enumerate()
        .map(lambda x: ("Vowels" if x[0] == 0 else "Consonants", [len(x[1]), x[1]]))
        .to_dict()
    )

process_str("123Ab5oc-E6db#bCi9<>")

# {'Vowels': [4, ('A', 'o', 'E', 'i')], 'Consonants': [6, ('b', 'c', 'd', 'b', 'b', 'C')]}
```

How hideous can it get?
<p align="center">
  <img src="https://github.com/kaliv0/pyrio/blob/main/assets/Chubby.jpg?raw=true" width="400" alt="Chubby">
</p>

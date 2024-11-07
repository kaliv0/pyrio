<p align="center">
  <img src="https://github.com/kaliv0/pyrio/blob/main/assets/Pyrio.jpg?raw=true" width="400" alt="Pyrio">
</p>

# PYRIO


![Python 3.x](https://img.shields.io/badge/python-3.12-blue?style=flat-square&logo=Python&logoColor=white)
[![tests](https://img.shields.io/github/actions/workflow/status/kaliv0/pyrio/ci.yml)](https://github.com/kaliv0/pyrio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kaliv0/pyrio/graph/badge.svg?token=7EEG43BL33)](https://codecov.io/gh/kaliv0/pyrio)
[![PyPI](https://img.shields.io/pypi/v/pyrio.svg)](https://pypi.org/project/pyrio/)
[![Downloads](https://static.pepy.tech/badge/pyrio)](https://pepy.tech/projects/pyrio)

<br><b>Functional-style Streams API library</b><br>
<br>Facilitates processing of collections and iterables using fluent APIs.
<br>Gives access to files of various types (<i>json</i>, <i>toml</i>, <i>yaml</i>, <i>xml</i>, <i>csv</i> and <i>tsv</i>) for reading and executing complex queries
<br>Provides easy integration with <i>itertools</i>
<br>(NB: Commonly used <i>itertools 'recipes'</i> are included as part of the main APIs)

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

- infinite unordered stream
```python
import random

Stream.generate(lambda: random.random())
```

- infinite stream with given value
```python
Stream.constant(42)
```

- concat
<br>(concatenate several streams together or add new streams to the current one)
```python
Stream.concat((1, 2, 3), [5, 6]).to_list()
Stream.of(1, 2, 3).concat([4, 5]).to_list()
```

- prepend
<br>(prepend iterable to current stream)
```python
Stream([2, 3, 4]).prepend(0, 1).to_list()
```
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
<br>(filter out all None or falsy values (if falsy=True) and applies mapper function to the elements of the stream)
```python
Stream.of(None, "foo", "", "bar", 0, []).filter_map(str.upper, falsy=True).to_list()
```
```shell
["FOO", "BAR"]
```

- flat_map
<br>(map each element of the stream and yields the elements of the produced iterators)
```python
Stream([[1, 2], [3, 4], [5]]).flat_map(lambda x: Stream(x)).to_list()
```
```shell
[1, 2, 3, 4, 5]
```

- flatten
```python
Stream([[1, 2], [3, 4], [5]]).flatten().to_list()
```
```shell
[1, 2, 3, 4, 5]
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
```
```shell
[1, 2, 3, 4]
```

- drop_while
<br>(returns a stream that skips elements based on a predicate and yields the remaining ones)
```python
Stream.of(1, 2, 3, 5, 6, 7, 2).drop_while(lambda x: x < 5).to_list()
```
```shell
[5, 6, 7, 2]
```

- sorted
<br>(sorts the elements of the current stream according to natural order or based on the given comparator;
<br>if 'reverse' flag is True, the elements are sorted in descending order)
```python
(Stream.of((3, 30), (2, 30), (2, 20), (1, 20), (1, 10))
    .sorted(lambda x: (x[0], x[1]), reverse=True)
    .to_list())
```
```shell
[(3, 30), (2, 30), (2, 20), (1, 20), (1, 10)]
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
```
```shell
{"fizz": 1, "buzz": 2}
```

In the case of a collision (duplicate keys) the 'merger' functions indicates which entry should be kept
```python
collection = [Foo("fizz", 1), Foo("fizz", 2), Foo("buzz", 2)]
Stream(collection).to_dict(collector=lambda x: (x.name, x.num), merger=lambda old, new: old)
```
```shell
{"fizz": 1, "buzz": 2}
```

- alternative for working with collectors is using the <i>collect</i> method
```python
Stream([1, 2, 3]).collect(tuple)
Stream.of(1, 2, 3).collect(list)
Stream.of(1, 1, 2, 2, 2, 3).collect(set)
Stream.of(1, 2, 3, 4).collect(dict, lambda x: (str(x), x * 10))
```

- grouping
```python
Stream("AAAABBBCCD").group_by(collector=lambda key, grouper: (key, len(grouper)))
```
```shell
{"A": 4, "B": 3, "C": 2, "D": 1}
```

```python
coll = [Foo("fizz", 1), Foo("fizz", 2), Foo("fizz", 3), Foo("buzz", 2), Foo("buzz", 3), Foo("buzz", 4), Foo("buzz", 5)]
Stream(coll).group_by(
    classifier=lambda obj: obj.name,
    collector=lambda key, grouper: (key, [(obj.name, obj.num) for obj in list(grouper)]))
```
```shell
{
  "fizz": [("fizz", 1), ("fizz", 2), ("fizz", 3)],
  "buzz": [("buzz", 2), ("buzz", 3), ("buzz", 4), ("buzz", 5)],
}
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
--------------------------------------------
### Itertools integration
```python
import itertools

Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).use(itertools.islice, start=3, stop=8)
```
#### Itertools 'recipes'
- tee
```python
Stream([1, 2, 3]).ncycles(count=2).to_list()
```
--------------------------------------------
### Querying files with FileStream
- working with <i>json</i>, <i>toml</i>, <i>yaml</i>, <i>xml</i> files
```python
FileStream("path/to/file").map(lambda x: f"{x.key}=>{x.value}").to_tuple()
```
```shell
(
  "abc=>xyz", 
  "qwerty=>42",
)
```
```python
from operator import itemgetter

(FileStream("path/to/file")
    .filter(lambda x: "a" in x.key)
    .map(lambda x: (x.key, sum(x.value) * 10))
    .sorted(itemgetter(1), reverse=True)
    .map(lambda x: f"{str(x[1])}::{x[0]}")
    .to_list()) 
```
```shell
["230::xza", "110::abba", "30::a"]
```
FileStream reads data as series of Item objects with key/value attributes.

- querying <i>csv</i> and <i>tsv</i> files
<br>(each row is read as a dict with keys taken from the header row)
```python
FileStream("path/to/file").map(lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}").to_tuple() 
```
```shell
(
  "fizz: 42, buzz: 45",
  "fizz: aaa, buzz: bbb",
)
```
```python
from operator import itemgetter

FileStream("path/to/file").map(itemgetter('fizz', 'buzz')).to_tuple()
```
```shell
(('42', '45'), ('aaa', 'bbb'))
```

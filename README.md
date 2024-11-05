<p align="center">
  <img src="https://github.com/kaliv0/pyrio/blob/main/assets/Pyrio.jpg?raw=true" width="400" alt="Pyrio">
</p>

# PYRIO


![Python 3.x](https://img.shields.io/badge/python-3.12-blue?style=flat-square&logo=Python&logoColor=white)
[![tests](https://img.shields.io/github/actions/workflow/status/kaliv0/pyrio/ci.yml)](https://github.com/kaliv0/pyrio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kaliv0/pyrio/graph/badge.svg?token=7EEG43BL33)](https://codecov.io/gh/kaliv0/pyrio)
[![PyPI](https://img.shields.io/pypi/v/pyrio.svg)](https://pypi.org/project/pyrio/)
[![Downloads](https://static.pepy.tech/badge/pyrio)](https://pepy.tech/project/pyrio)

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

- filter map
  <br>(Filters out all None or falsy values (if falsy=True) and applies mapper function to the elements of the stream)
```python
Stream.of(None, "foo", "", "bar", 0, []).filter_map(str.upper, falsy=True).to_list()
```
```shell
["FOO", "BAR"]
```

- reduce (similarly to Java returns Optional)
```python
Stream([1, 2, 3]).reduce(lambda acc, val: acc + val, identity=3).get()
```
--------------------------------------------
### Terminal operations
- to_list
```python
Stream([1, 2, 3]).to_list()
```
--------------------------------------------
## Itertools integration
```python
import itertools
Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).use(itertools.islice, start=3, stop=8)
```
### Itertools 'recipes'
- tee
```python
Stream([1, 2, 3]).ncycles(count=2).to_list()
```
<p align="center">
  <img src="https://github.com/kaliv0/pyrio/blob/main/assets/Pyrio.jpg?raw=true" width="400" alt="Pyrio">
</p>

# PYRIO


![Python 3.x](https://img.shields.io/badge/python-3.12-blue?style=flat-square&logo=Python&logoColor=white)
[![tests](https://img.shields.io/github/actions/workflow/status/kaliv0/pyrio/ci.yml)](https://github.com/kaliv0/pyrio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kaliv0/pyrio/graph/badge.svg?token=7EEG43BL33)](https://codecov.io/gh/kaliv0/pyrio)
[![PyPI](https://img.shields.io/pypi/v/pyrio.svg)](https://pypi.org/project/pyrio/)
[![Downloads](https://static.pepy.tech/badge/pyrio)](https://pepy.tech/project/pyrio)

<br><b>Java-inspired Streams Api library</b>
<br>Provides easy integration with <i>itertools</i>
<br>Commonly used <i>itertools 'recipes'</i> are included as part of the main APIs

## How to use
### creating streams
- stream from iterable
```python
Stream([1, 2, 3])
```
- empty stream
```python
Stream.empty()
```
--------------------------------------------
### intermediate operations
- filter
```python
Stream([1, 2, 3]).filter(lambda x: x % 2 == 0)
```
--------------------------------------------
### terminal operations
- to_list
```python
Stream([1, 2, 3]).to_list()
```
--------------------------------------------
## itertools integration

```python
import itertools

Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).use(itertools.islice, start=3, stop=8)
```
### itertools 'recipes'
- tee
```python
Stream([1, 2, 3]).ncycles(count=2).to_list()
```

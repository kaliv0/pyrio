import csv
import json
import tomllib
from dataclasses import dataclass
from typing import Any

import yaml

from pyrio import Stream

@dataclass(frozen=True)
class Item:
    key: Any
    value: Any


def test_json():
    with open("tests/resources/foo.json", 'rb') as f:
        contents = json.load(f)
    assert Stream((Item(k, v) for k, v in contents.items())).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == ('abc=>xyz', 'qwerty=>42')

def test_toml():
    with open("tests/resources/bar.toml", 'rb') as f:
        contents = tomllib.load(f)
    assert Stream(contents.items()).map(lambda x: f"{x[0]}=>{x[1]}").to_tuple() == ('abc=>xyz', 'qwerty=>42')

def test_yaml():
    with open("tests/resources/bazz.yaml", 'rb') as f:
        contents = yaml.safe_load(f)
    assert Stream.of(*(contents.items())).map(lambda x: f"{x[0]}=>{x[1]}").to_tuple() == ('abc=>xyz', 'qwerty=>42')

def test_csv():
    with (open("tests/resources/fizz.csv", newline='') as f):
        reader = csv.DictReader(f)
        assert Stream(list(reader)).map(lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}").to_tuple() == \
               ('fizz: 42, buzz: 45', 'fizz: aaa, buzz: bbb')

def test_nasty_json():
    with open("tests/resources/nasty.json", 'rb') as f:
        contents = json.load(f)
    assert (Stream((Item(k, v) for k, v in contents.items()))
            .map(lambda x: Item('second', x.value['second']))
            .map(lambda x: x.value)
            .flatten().to_list() == [1, 2, 3, 4])

    assert (Stream((Item(k, v) for k, v in contents.items()))
            .map(lambda x: x.value['second'])
            .flatten().to_list() == [1, 2, 3, 4])

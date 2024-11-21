from operator import itemgetter
from pathlib import Path

import pytest

from pyrio import FileStream, Stream
from pyrio.exception import IllegalStateError, UnsupportedFileTypeError
from pyrio.base_stream import Item


def test_invalid_path_error():
    file_path = "./foo/bar.xyz"
    with pytest.raises(FileNotFoundError) as e:
        FileStream.of(file_path)
    assert str(e.value) == f"No such file or directory: '{file_path}'"


def test_path_is_dir_error():
    file_path = "./tests/resources/"
    with pytest.raises(IsADirectoryError) as e:
        FileStream.of(file_path)
    assert str(e.value) == f"Given path '{file_path}' is a directory"


@pytest.mark.parametrize(
    "file_path",
    [
        "./tests/resources/fizz.buzz",
        "./tests/resources/noextension",
        "./tests/resources/empty.",
    ],
)
def test_file_type_error(file_path):
    with pytest.raises(UnsupportedFileTypeError) as e:
        FileStream.of(file_path)
    assert str(e.value) == f"Unsupported file type: '{Path(file_path).suffix}'"


@pytest.mark.parametrize(
    "file_path",
    [
        "./tests/resources/foo.json",
        "./tests/resources/foo.toml",
        "./tests/resources/foo.yaml",
        "./tests/resources/foo.xml",
    ],
)
def test_json(file_path):
    assert FileStream.of(file_path).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


@pytest.mark.parametrize(
    "file_path",
    [
        "./tests/resources/bar.csv",
        "./tests/resources/bar.tsv",
    ],
)
def test_csv(file_path):
    assert FileStream.of_csv(file_path).map(lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}").to_tuple() == (
        "fizz: 42, buzz: 45",
        "fizz: aaa, buzz: bbb",
    )


def test_nested_json():
    assert FileStream.of("./tests/resources/nested.json").map(
        lambda x: x.value["second"]
    ).flatten().to_list() == [
        1,
        2,
        3,
        4,
    ]


def test_complex_pipeline():
    assert (
        FileStream.of("./tests/resources/long.json")
        .filter(lambda x: "a" in x.key)
        .map(lambda x: (x.key, sum(x.value) * 10))
        .sorted(itemgetter(1), reverse=True)
        .map(lambda x: f"{str(x[1])}::{x[0]}")
    ).to_list() == ["230::xza", "110::abba", "30::a"]


def test_reusing_stream():
    stream = FileStream.of("./tests/resources/foo.json")
    assert stream._is_consumed is False

    result = stream.map(lambda x: f"{x.key}=>{x.value}").tail(1).to_tuple()
    assert result == ("qwerty=>42",)
    assert stream._is_consumed

    with pytest.raises(IllegalStateError) as e:
        stream.map(lambda x: x.value * 10).to_list()
    assert str(e.value) == "Stream object already consumed"


def test_concat():
    assert (
        FileStream.of("./tests/resources/long.json")
        .concat(FileStream.of("./tests/resources/foo.json"))
        .map(lambda x: f"{x.key}: {x.value}")
    ).to_tuple() == (
        "a: [1, 2]",
        "b: [2, 3, 4]",
        "abba: [5, 6]",
        "x: []",
        "y: [55]",
        "xza: [11, 12]",
        "z: [3]",
        "zzz: None",
        "abc: xyz",
        "qwerty: 42",
    )


def test_prepend():
    json_dict = (
        Stream(
            {
                "Name": "Jennifer Smith",
                "Security Number": 7867567898,
                "Phone": "555-123-4568",
                "Email": "jen123@gmail.com",
                "Hobbies": ["Reading", "Sketching", "Horse Riding"],
            }
        )
        .map(lambda x: Item(x.key, x.value))
        .to_tuple()
    )
    assert (
        FileStream.of("./tests/resources/long.json")
        .prepend(json_dict)
        .map(lambda x: f"key={x.key}, value={x.value}")
    ).to_tuple() == (
        "key=Name, value=Jennifer Smith",
        "key=Security Number, value=7867567898",
        "key=Phone, value=555-123-4568",
        "key=Email, value=jen123@gmail.com",
        "key=Hobbies, value=['Reading', 'Sketching', 'Horse Riding']",
        "key=a, value=[1, 2]",
        "key=b, value=[2, 3, 4]",
        "key=abba, value=[5, 6]",
        "key=x, value=[]",
        "key=y, value=[55]",
        "key=xza, value=[11, 12]",
        "key=z, value=[3]",
        "key=zzz, value=None",
    )

import pytest

from pyrio.exception import IllegalStateError, UnsupportedFileTypeError
from pyrio.file_stream import FileStream


def test_invalid_path_error():
    file_path = "./foo/bar.xyz"
    with pytest.raises(FileNotFoundError) as e:
        FileStream(file_path)
    assert str(e.value) == f"No such file or directory: '{file_path}'"


def test_path_is_dir_error():
    file_path = "./tests/resources/"
    with pytest.raises(IsADirectoryError) as e:
        FileStream(file_path)
    assert str(e.value) == f"Given path '{file_path}' is a directory"


def test_file_type_error():
    with pytest.raises(UnsupportedFileTypeError) as e:
        FileStream("./tests/resources/fizz.buzz")
    assert str(e.value) == "Unsupported file type: '.buzz'"


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
    assert FileStream(file_path).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
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
    assert FileStream(file_path).map(lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}").to_tuple() == (
        "fizz: 42, buzz: 45",
        "fizz: aaa, buzz: bbb",
    )


def test_nested_json():
    assert FileStream("./tests/resources/nested.json").map(
        lambda x: x.value["second"]
    ).flatten().to_list() == [
        1,
        2,
        3,
        4,
    ]


def test_complex_pipeline():
    assert (
        FileStream("./tests/resources/long.json")
        .filter(lambda x: "a" in x.key)
        .map(lambda x: (x.key, sum(x.value) * 10))
        .sorted(lambda x: x[1], reverse=True)
        .map(lambda x: f"{str(x[1])}::{x[0]}")
    ).to_list() == ["230::xza", "110::abba", "30::a"]


def test_reusing_stream():
    stream = FileStream("./tests/resources/foo.json")
    assert stream._is_consumed is False

    result = stream.map(lambda x: f"{x.key}=>{x.value}").tail(1).to_tuple()
    assert result == ("qwerty=>42",)
    assert stream._is_consumed

    with pytest.raises(IllegalStateError) as e:
        stream.map(lambda x: x.value * 10).to_list()
    assert str(e.value) == "Stream object already consumed"

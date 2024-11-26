from decimal import Decimal
from operator import attrgetter
from pathlib import Path

import pytest

from pyrio import FileStream, Stream, Item
from pyrio.utils.exception import IllegalStateError, UnsupportedFileTypeError


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
        FileStream(file_path)
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
def test_read_files(file_path):
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
    assert FileStream(file_path).map(
        lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}"
    ).to_tuple() == (
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


def test_nested_dict():
    assert (
        FileStream("./tests/resources/nested_dicts.json")
        .filter(lambda outer: "user" not in outer.key)
        .map(
            lambda outer: (
                Stream(outer.value)
                .filter(lambda inner: inner.key == "Email")
                .map(
                    lambda inner: (
                        Stream(inner.value)
                        .filter(lambda deepest: deepest.key == "primary")
                        .map(lambda deepest: deepest.value)
                        .to_tuple()
                    )
                )
                .to_tuple()
            )
        )
        .flatten()
        .to_list()
    ) == ["johnny@bravo.cash", "ziggy@psycho.au"]


def test_complex_pipeline():
    assert (
        FileStream("./tests/resources/long.json")
        .filter(lambda x: "a" in x.key)
        .map(lambda x: Item(x.key, sum(x.value) * 10))
        .sorted(attrgetter("value"), reverse=True)
        .map(lambda x: f"{str(x.value)}::{x.key}")
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


def test_concat():
    assert (
        FileStream("./tests/resources/long.json")
        .concat(FileStream("./tests/resources/foo.json"))
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


def test_prepend(json_dict):
    in_memory_dict = Stream(json_dict).to_tuple()
    assert (
        FileStream("./tests/resources/long.json")
        .prepend(in_memory_dict)
        .map(lambda x: f"key={x.key}, value={x.value}")
    ).to_tuple() == (
        "key=Name, value=Jennifer Smith",
        "key=Security_Number, value=7867567898",
        "key=Phone, value=555-123-4568",
        "key=Email, value={'primary': 'jen123@gmail.com'}",
        "key=Hobbies, value=['Reading', 'Sketching', 'Horse Riding']",
        "key=Job, value=None",
        "key=a, value=[1, 2]",
        "key=b, value=[2, 3, 4]",
        "key=abba, value=[5, 6]",
        "key=x, value=[]",
        "key=y, value=[55]",
        "key=xza, value=[11, 12]",
        "key=z, value=[3]",
        "key=zzz, value=None",
    )


@pytest.mark.parametrize(
    "file_path",
    ["./tests/resources/parse_float.toml", "./tests/resources/parse_float.json"],
)
def test_process(file_path):
    def check_type(x):
        match x.key:
            case "a":
                return isinstance(x.value, str)
            case "b":
                return isinstance(x.value, bool)
            case "x" | "y":
                return isinstance(x.value, Decimal)
            case _:
                return False

    assert FileStream.process(file_path, parse_float=Decimal).all_match(check_type)


# ### save to file ###
def test_save_toml(tmp_file_dir, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / "test.toml"
    FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
        tmp_file_path,
        null_handler=lambda x: Item(x.key, "Unknown") if x.value is None else x,
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8")
        == open("./tests/resources/save_output/test.toml").read()
    )


def test_save_toml_default_null_handler(tmp_file_dir, json_dict):
    in_memory_dict = Stream(json_dict).to_tuple()
    tmp_file_path = tmp_file_dir / "test_default_null_handler.toml"
    FileStream("./tests/resources/foo.toml").concat(in_memory_dict).save(tmp_file_path)
    assert (
        tmp_file_path.read_text()
        == open("./tests/resources/save_output/test_default_null_handler.toml").read()
    )


@pytest.mark.parametrize(
    "file_path, indent",
    [("test.json", 2), ("test.yaml", 2), ("test.xml", 4)],
)
def test_save(tmp_file_dir, file_path, indent, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / file_path
    FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
        tmp_file_path,
        f_open_options={"encoding": "utf-8"},
        f_save_options={"indent": indent},
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8")
        == open(f"./tests/resources/save_output/{file_path}").read()
    )


@pytest.mark.parametrize(
    "file_path, indent",
    [("test_null_handler.json", 2), ("test_null_handler.yaml", 2), ("test_null_handler.xml", 4)],
)
def test_save_handle_null(tmp_file_dir, file_path, indent, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / file_path
    FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
        tmp_file_path,
        null_handler=lambda x: Item(x.key, "Unknown") if x.value is None else x,
        f_open_options={"encoding": "utf-8"},
        f_save_options={"indent": indent},
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8")
        == open(f"./tests/resources/save_output/{file_path}").read()
    )

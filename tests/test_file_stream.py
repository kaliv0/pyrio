import pytest

from pyrio.file_stream import FileStream


def test_invalid_path_error():
    with pytest.raises(FileNotFoundError) as e:
        FileStream("./foo/bar.xyz")
    assert str(e.value) == "No such file or directory: './foo/bar.xyz'"


def test_path_is_dir_error():
    with pytest.raises(IsADirectoryError) as e:
        FileStream("./tests/resources/")
    assert str(e.value) == "Given path './tests/resources/' is a directory"


def test_json():
    assert FileStream("./tests/resources/foo.json").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_toml():
    assert FileStream("./tests/resources/bar.toml").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_yaml():
    assert FileStream("./tests/resources/bazz.yaml").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_xml():
    assert FileStream("./tests/resources/boo.xml").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_csv():
    assert FileStream("./tests/resources/fizz.csv").map(
        lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}"
    ).to_tuple() == (
        "fizz: 42, buzz: 45",
        "fizz: aaa, buzz: bbb",
    )


def test_tsv():
    assert FileStream("./tests/resources/buzz.tsv").map(
        lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}"
    ).to_tuple() == (
        "fizz: 42, buzz: 45",
        "fizz: aaa, buzz: bbb",
    )


def test_nasty_json():
    assert FileStream("./tests/resources/nasty.json").map(
        lambda x: x.value["second"]
    ).flatten().to_list() == [
        1,
        2,
        3,
        4,
    ]

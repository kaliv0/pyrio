import shutil
from decimal import Decimal
from operator import attrgetter
from pathlib import Path

import pytest

from pyrio import FileStream, Stream, DictItem
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


def test_read_yml():
    assert FileStream("./tests/resources/foo.yml").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_read_xml_custom_root():
    assert FileStream("./tests/resources/custom_root.xml").map(
        lambda x: f"{x.key}=>{x.value}"
    ).to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_read_xml_include_root():
    assert FileStream.process("./tests/resources/custom_root.xml", include_root=True).map(
        lambda x: f"root={x.key}: inner_records={str(Stream(x.value).to_dict())}"
    ).to_list() == ["root=my-root: inner_records={'abc': 'xyz', 'qwerty': '42'}"]


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
    assert FileStream("./tests/resources/nested.json").map(lambda x: x.value).flat_map(
        lambda x: Stream(x).filter(lambda y: y.key == "second").flat_map(lambda z: z.value).to_tuple()
    ).to_list() == [
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
        .map(lambda x: DictItem(x.key, sum(x.value) * 10))
        .sort(attrgetter("value"), reverse=True)
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
        "key=Email, value=(DictItem(key=primary, value=jen123@gmail.com),)",
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

    assert FileStream.process(file_path, f_read_options={"parse_float": Decimal}).all_match(check_type)


# ### save to file ###
def test_save_toml(tmp_file_dir, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / "test.toml"
    FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
        tmp_file_path,
        null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
    )
    assert tmp_file_path.read_text(encoding="utf-8") == open("./tests/resources/save_output/test.toml").read()


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
    [("test.json", 2), ("test.yaml", 2), ("test.yml", 2), ("test.xml", 4)],
)
def test_save(tmp_file_dir, file_path, indent, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / file_path
    FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
        tmp_file_path,
        f_open_options={"encoding": "utf-8"},
        f_write_options={"indent": indent},
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8") == open(f"./tests/resources/save_output/{file_path}").read()
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
        null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
        f_open_options={"encoding": "utf-8"},
        f_write_options={"indent": indent},
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8") == open(f"./tests/resources/save_output/{file_path}").read()
    )


def test_save_custom_xml_root(tmp_file_dir, json_dict):
    file_path = "custom_root.xml"
    tmp_file_path = tmp_file_dir / file_path
    indent = 4

    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
        tmp_file_path,
        null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
        f_open_options={"encoding": "utf-8"},
        f_write_options={"indent": indent},
        xml_root="my-root",
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8") == open(f"./tests/resources/save_output/{file_path}").read()
    )


def test_update_file(tmp_file_dir, json_dict):
    tmp_file_path = tmp_file_dir / "updated.json"
    shutil.copyfile("./tests/resources/long.json", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .map(lambda x: DictItem(x.key, ", ".join((str(y) for y in x.value)) if x.value else x.value))
        .save(
            null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
            f_write_options={"indent": 2},
        )
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8") == open("./tests/resources/save_output/updated.json").read()
    )


def test_filter_update_file(tmp_file_dir, json_dict):
    tmp_file_path = tmp_file_dir / "filtered.toml"
    shutil.copyfile("./tests/resources/test.toml", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .filter(lambda x: isinstance(x.value, str))
        .reverse(comparator=lambda x: x.key)
        .save()
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8")
        == open("./tests/resources/save_output/filtered.toml").read()
    )


@pytest.mark.parametrize(
    "file_path",
    ["test.csv", "test.tsv"],
)
def test_save_csv(tmp_file_dir, file_path):
    tmp_file_path = tmp_file_dir / file_path
    FileStream("./tests/resources/bar.csv").save(tmp_file_path)
    assert (
        tmp_file_path.read_text(encoding="utf-8") == open(f"./tests/resources/save_output/{file_path}").read()
    )


def test_save_convert_to_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "converted.csv"
    (
        FileStream("./tests/resources/convertable.json")
        .filter(
            lambda x: (
                Stream(x.value)
                .find_first(lambda y: y.key == "name" and y.value == "Snake")
                .or_else_get(lambda: None)
            )
            is None
        )
        .map(lambda x: x.value)
        .save(tmp_file_path)
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8")
        == open("./tests/resources/save_output/converted.csv").read()
    )


def test_save_to_csv_with_null_handler(tmp_file_dir):
    def _null_handler(dict_obj):
        return Stream(dict_obj).to_dict(lambda x: DictItem(x.key, x.value or "N/A"))

    tmp_file_path = tmp_file_dir / "converted_null.csv"
    (
        FileStream("./tests/resources/convertable.json")
        .filter(
            lambda x: (
                Stream(x.value)
                .find_first(lambda y: y.key == "name" and y.value == "Snake")
                .or_else_get(lambda: None)
            )
        )
        .map(lambda x: x.value)
        .save(tmp_file_path, null_handler=_null_handler)
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8")
        == open("./tests/resources/save_output/converted_null.csv").read()
    )


def test_save_empty_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "dead.csv"
    shutil.copyfile("./tests/resources/bar.csv", tmp_file_path)
    stream = FileStream(tmp_file_path)
    stream._iterable = tuple()
    stream.save()
    assert tmp_file_path.read_text(encoding="utf-8") == open("./tests/resources/save_output/empty.csv").read()


def test_update_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "updated.csv"
    shutil.copyfile("./tests/resources/editable.csv", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .map(lambda x: (Stream(x).to_dict(lambda y: DictItem(y.key, y.value or "Unknown"))))
        .save(tmp_file_path)
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8") == open("./tests/resources/save_output/updated.csv").read()
    )


def test_update_fails(tmp_file_dir):
    def _raise(exception):
        raise exception

    tmp_file_path = tmp_file_dir / "fail.csv"
    shutil.copyfile("./tests/resources/editable.csv", tmp_file_path)
    with pytest.raises(IOError, match="Ooops Mr White..."):
        FileStream(tmp_file_path).save(tmp_file_path, null_handler=_raise(IOError("Ooops Mr White...")))
    assert tmp_file_path.read_text(encoding="utf-8") == open("./tests/resources/editable.csv").read()


def test_combine_files_into_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "merged.csv"
    shutil.copyfile("./tests/resources/combine.csv", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .concat(
            FileStream("./tests/resources/convertable.json")
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
        .save(tmp_file_path)
    )
    assert (
        tmp_file_path.read_text(encoding="utf-8") == open("./tests/resources/save_output/merged.csv").read()
    )

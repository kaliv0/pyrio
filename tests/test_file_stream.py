import json
import pickle
import shutil
from configparser import MissingSectionHeaderError
from decimal import Decimal
from json import JSONDecodeError
from operator import attrgetter
from pathlib import Path
from tomllib import TOMLDecodeError
from xml.parsers.expat import ExpatError

import pytest
from yaml.parser import ParserError

from pyrio import DictItem, FileStream, Stream
from pyrio.exceptions import IllegalStateError, NoneTypeError

INPUT = Path("./tests/resources/input")
EXPECTED = Path("./tests/resources/expected")

MAPPING_SUFFIXES = [".json", ".yaml", ".xml", ".toml", ".ini"]
DSV_SUFFIXES = [".csv", ".tsv"]


def test_none_path_error():
    with pytest.raises(NoneTypeError) as e:
        FileStream(None)
    assert str(e.value) == "File path cannot be None"


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


@pytest.mark.parametrize("suffix", MAPPING_SUFFIXES)
def test_read_files(suffix):
    assert FileStream(_input("flat", "foo", suffix)).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_yml_alias():
    assert FileStream(_input("flat", "foo", ".yml")).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        FileStream(_input("flat", "foo", ".yaml")).map(lambda x: f"{x.key}=>{x.value}").to_tuple()
    )


def test_cfg_alias():
    assert FileStream(_input("flat", "foo", ".cfg")).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        FileStream(_input("flat", "foo", ".ini")).map(lambda x: f"{x.key}=>{x.value}").to_tuple()
    )


def test_pkl_alias(tmp_file_dir):
    data = {"abc": "xyz"}
    pkl = tmp_file_dir / "alias.pkl"
    pkl.write_bytes(pickle.dumps(data))
    assert FileStream(pkl).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == ("abc=>xyz",)


def test_ini_default_section_not_leaked():
    # ConfigParser inherits [DEFAULT] into every section via items() -> load must ignore that
    assert FileStream(_input("options", "default_section", ".ini")).to_dict() == {
        "Name": "Alice",
        "child": {"x": "1"},
    }


def test_read_xml_custom_root():
    assert FileStream(_input("options", "custom_root", ".xml")).map(
        lambda x: f"{x.key}=>{x.value}"
    ).to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_read_xml_include_root():
    assert FileStream.process(_input("options", "custom_root", ".xml"), include_root=True).map(
        lambda x: f"root={x.key}: inner_records={str(Stream(x.value).to_dict())}"
    ).to_list() == ["root=my-root: inner_records={'abc': 'xyz', 'qwerty': '42'}"]


@pytest.mark.parametrize("suffix", DSV_SUFFIXES)
def test_dsv(suffix):
    assert FileStream(str(INPUT / "dsv" / f"bar{suffix}")).map(
        lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}"
    ).to_tuple() == (
        "fizz: 42, buzz: 45",
        "fizz: aaa, buzz: bbb",
    )


def test_read_plain_text():
    lorem = FileStream(str(INPUT / "plain" / "plain.txt"))
    assert lorem.map(lambda x: x.strip()).to_string("||") == (
        "Lorem ipsum dolor sit amet, consectetur adipisicing elit,"
        "||sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
        "||Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris"
        "||nisi ut aliquip ex ea commodo consequat."
        "||Duis aute irure dolor in reprehenderit in voluptate velit esse"
        "||cillum dolore eu fugiat nulla pariatur."
        "||Excepteur sint occaecat cupidatat non proident, sunt in culpa"
        "||qui officia deserunt mollit anim id est laborum."
    )


def test_read_plain_and_query():
    assert FileStream(str(INPUT / "plain" / "plain.txt")).map(lambda x: x.strip()).enumerate().filter(
        lambda line: "id" in line[1]
    ).to_dict() == {
        1: "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
        6: "Excepteur sint occaecat cupidatat non proident, sunt in culpa",
        7: "qui officia deserunt mollit anim id est laborum.",
    }


@pytest.mark.parametrize("suffix", MAPPING_SUFFIXES)
def test_nested(suffix):
    assert FileStream(_input("nested", "nested", suffix)).map(lambda x: x.value).flat_map(
        lambda x: Stream(x).filter(lambda y: y.key == "second").flat_map(lambda z: z.value).to_tuple()
    ).to_list() == ["x", "y", "z"]


@pytest.mark.parametrize("suffix", MAPPING_SUFFIXES)
def test_nested_dict(suffix):
    assert (
        FileStream(_input("nested", "nested_dicts", suffix))
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


@pytest.mark.parametrize("suffix", MAPPING_SUFFIXES)
def test_nested_filter_keys(suffix):
    assert (
        FileStream(_input("nested", "nested", suffix))
        .filter(lambda x: x.key == "first")
        .map(lambda x: x.key)
        .to_list()
    ) == ["first"]


def test_complex_pipeline():
    assert (
        FileStream(_input("nested", "long", ".json"))
        .filter(lambda x: "a" in x.key)
        .map(lambda x: DictItem(x.key, sum(x.value) * 10))
        .sort(attrgetter("value"), reverse=True)
        .map(lambda x: f"{str(x.value)}::{x.key}")
    ).to_list() == ["230::xza", "110::abba", "30::a"]


def test_reusing_stream():
    stream = FileStream(_input("flat", "foo", ".json"))
    assert stream._is_consumed is False

    result = stream.map(lambda x: f"{x.key}=>{x.value}").tail(1).to_tuple()
    assert result == ("qwerty=>42",)
    assert stream._is_consumed
    assert stream._file_handler.closed

    with pytest.raises(IllegalStateError) as e:
        stream.map(lambda x: x.value * 10).to_list()
    assert str(e.value) == "Stream object already consumed"


def test_save_marks_stream_consumed(tmp_file_dir):
    stream = FileStream(_input("flat", "foo", ".json"))
    stream.save(tmp_file_dir / "out.json")
    assert stream._is_consumed
    assert stream._file_handler.closed
    with pytest.raises(IllegalStateError) as e:
        stream.to_list()
    assert str(e.value) == "Stream object already consumed"


def test_terminal_closes_even_on_error():
    def boom(_):
        raise ValueError("boom")

    stream = FileStream(_input("flat", "foo", ".json"))
    with pytest.raises(ValueError, match="boom"):
        stream.for_each(boom)

    assert stream._is_consumed
    assert stream._file_handler.closed
    with pytest.raises(IllegalStateError):
        stream.to_list()


def test_concat():
    assert (
        FileStream(_input("nested", "long", ".json"))
        .concat(FileStream(_input("flat", "foo", ".json")))
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
        FileStream(_input("nested", "long", ".json"))
        .prepend(in_memory_dict)
        .map(lambda x: f"key={x.key}, value={x.value}")
    ).to_tuple() == (
        "key=Name, value=Jennifer Smith",
        "key=Security_Number, value=7867567898",
        "key=Phone, value=555-123-4568",
        "key=Email, value=(DictItem(key='primary', value='jen123@gmail.com'),)",
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


@pytest.mark.parametrize("suffix", [".toml", ".json", ".yaml"])
def test_process(suffix):
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

    assert FileStream.process(
        _input("options", "parse_float", suffix), f_read={"parse_float": Decimal}
    ).all_match(check_type)


def test_save_toml(tmp_file_dir, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / "test.toml"
    FileStream(_input("nested", "nested", ".json")).prepend(in_memory_dict).save(
        tmp_file_path,
        null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
    )
    assert tmp_file_path.read_text() == (EXPECTED / "save" / "test.toml").read_text()


def test_save_toml_default_null_handler(tmp_file_dir, json_dict):
    in_memory_dict = Stream(json_dict).to_tuple()
    tmp_file_path = tmp_file_dir / "test_default_null_handler.toml"
    FileStream(_input("flat", "foo", ".toml")).concat(in_memory_dict).save(tmp_file_path)
    assert tmp_file_path.read_text() == (EXPECTED / "null" / "test_default_null_handler.toml").read_text()


@pytest.mark.parametrize(
    "file_path, indent",
    [("test.json", 2), ("test.yaml", 2), ("test.xml", 4)],
)
def test_save(tmp_file_dir, file_path, indent, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / file_path
    FileStream(_input("nested", "nested", ".json")).prepend(in_memory_dict).save(
        tmp_file_path,
        f_open={"encoding": "utf-8"},
        f_write={"indent": indent},
    )
    assert tmp_file_path.read_text() == (EXPECTED / "save" / file_path).read_text()


@pytest.mark.parametrize(
    "file_path, indent",
    [
        ("test_null_handler.json", 2),
        ("test_null_handler.yaml", 2),
        ("test_null_handler.xml", 4),
        ("test_null_handler.toml", None),
        ("test_null_handler.ini", None),
    ],
)
def test_save_handle_null(tmp_file_dir, file_path, indent, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / file_path
    f_write = {"indent": indent} if indent is not None else {}
    f_open = {} if file_path.endswith((".toml", ".ini")) else {"encoding": "utf-8"}
    FileStream(_input("nested", "nested", ".json")).prepend(in_memory_dict).save(
        tmp_file_path,
        f_open=f_open,
        f_write=f_write,
        null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
    )
    assert tmp_file_path.read_text() == (EXPECTED / "null" / file_path).read_text()


def test_save_ini(tmp_file_dir, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    tmp_file_path = tmp_file_dir / "test.ini"
    FileStream(_input("nested", "nested", ".json")).prepend(in_memory_dict).save(
        tmp_file_path,
        null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
    )
    assert tmp_file_path.read_text() == (EXPECTED / "save" / "test.ini").read_text()


def test_save_custom_xml_root(tmp_file_dir, json_dict):
    file_path = "custom_root.xml"
    tmp_file_path = tmp_file_dir / file_path
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    FileStream(_input("nested", "nested", ".json")).prepend(in_memory_dict).save(
        tmp_file_path,
        f_write={"indent": 4},
        null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
        xml_root="my-root",
    )
    assert tmp_file_path.read_text() == (EXPECTED / "save" / file_path).read_text()


def test_save_plain(tmp_file_dir):
    file_path = "lorem.txt"
    tmp_file_path = tmp_file_dir / "lorem.txt"
    fs = FileStream(str(INPUT / "plain" / "plain.txt"))
    (
        fs.map(lambda line: line.strip())
        .enumerate()
        .filter(lambda line: "x" in line[1])
        .map(lambda line: f"line_num:{line[0]}, text='{line[1]}'")
        .save(tmp_file_path)
    )
    assert tmp_file_path.read_text() == (EXPECTED / "plain" / file_path).read_text()
    assert fs._file_handler.closed


def test_save_raises():
    with pytest.raises(UnicodeDecodeError) as e:
        FileStream(_input("options", "awake", ".mp3")).save("./tests/resources/woke.json")
    assert str(e.value) == "'utf-8' codec can't decode byte 0xff in position 45: invalid start byte"


def test_update_plain(tmp_file_dir, json_dict):
    file_path = "lorem.txt"
    tmp_file_path = tmp_file_dir / file_path
    shutil.copyfile(INPUT / "plain" / "plain.txt", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .map(lambda line: line.strip())
        .enumerate()
        .filter(lambda line: "x" in line[1])
        .map(lambda line: f"line_num:{line[0]}, text='{line[1]}'")
        .save()
    )
    assert tmp_file_path.read_text() == (EXPECTED / "plain" / file_path).read_text()


def test_update_file(tmp_file_dir, json_dict):
    tmp_file_path = tmp_file_dir / "updated.json"
    shutil.copyfile(_input("nested", "long", ".json"), tmp_file_path)
    (
        FileStream(tmp_file_path)
        .map(lambda x: DictItem(x.key, ", ".join((str(y) for y in x.value)) if x.value else x.value))
        .save(
            f_write={"indent": 2},
            null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
        )
    )
    assert tmp_file_path.read_text() == (EXPECTED / "update" / "updated.json").read_text()


@pytest.mark.parametrize("suffix", MAPPING_SUFFIXES)
def test_update_filter_keys(tmp_file_dir, suffix):
    tmp_file_path = tmp_file_dir / f"foo{suffix}"
    shutil.copyfile(_input("flat", "foo", suffix), tmp_file_path)
    f_open = {} if suffix == ".toml" else {"encoding": "utf-8"}
    f_write = {} if suffix == ".toml" else {"indent": 2}
    FileStream(tmp_file_path).filter(lambda x: x.key == "abc").save(
        tmp_file_path, f_open=f_open, f_write=f_write
    )
    assert FileStream(tmp_file_path).map(lambda x: x.key).to_list() == ["abc"]


@pytest.mark.parametrize("suffix", MAPPING_SUFFIXES)
def test_filter_update_file(tmp_file_dir, suffix):
    file_name = f"filtered{suffix}"
    tmp_file_path = tmp_file_dir / file_name
    shutil.copyfile(_input("nested", "test", suffix), tmp_file_path)
    (
        FileStream(tmp_file_path)
        .filter(lambda x: isinstance(x.value, str))
        .reverse(comparator=lambda x: x.key)
        .save()
    )
    assert tmp_file_path.read_text() == (EXPECTED / "update" / file_name).read_text()


@pytest.mark.parametrize("suffix", MAPPING_SUFFIXES)
def test_round_trip_mapping(tmp_file_dir, suffix):
    src = _input("flat", "foo", suffix)
    tmp = tmp_file_dir / f"round{suffix}"
    f_open = {} if suffix == ".toml" else {"encoding": "utf-8"}
    f_write = {} if suffix == ".toml" else {"indent": 2}
    FileStream(src).save(tmp, f_open=f_open, f_write=f_write)
    assert (
        FileStream(tmp).map(lambda x: (x.key, str(x.value))).to_list()
        == FileStream(src).map(lambda x: (x.key, str(x.value))).to_list()
    )


@pytest.mark.parametrize("suffix", [".json", ".yaml", ".toml"])
def test_round_trip_unicode(tmp_file_dir, suffix):
    src = _input("options", "unicode", suffix)
    tmp = tmp_file_dir / f"unicode{suffix}"
    f_open = {} if suffix == ".toml" else {"encoding": "utf-8"}
    f_write = {} if suffix == ".toml" else {"indent": 2}
    FileStream(src).save(tmp, f_open=f_open, f_write=f_write)
    assert FileStream(tmp).to_dict() == FileStream(src).to_dict()


def test_round_trip_csv(tmp_file_dir):
    src = str(INPUT / "dsv" / "bar.csv")
    tmp = tmp_file_dir / "round.csv"
    FileStream(src).save(tmp)
    assert FileStream(tmp).to_list() == FileStream(src).to_list()


def test_round_trip_plain(tmp_file_dir):
    src = str(INPUT / "plain" / "plain.txt")
    tmp = tmp_file_dir / "round.txt"
    FileStream(src).map(lambda x: x.strip()).save(tmp)
    assert (
        FileStream(tmp).map(lambda x: x.strip()).to_list()
        == FileStream(src).map(lambda x: x.strip()).to_list()
    )


@pytest.mark.parametrize("suffix", DSV_SUFFIXES)
def test_save_csv(tmp_file_dir, suffix):
    file_path = f"test{suffix}"
    tmp_file_path = tmp_file_dir / file_path
    FileStream(str(INPUT / "dsv" / "bar.csv")).save(tmp_file_path)
    assert tmp_file_path.read_text() == (EXPECTED / "save" / file_path).read_text()


def test_save_convert_to_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "converted.csv"
    (
        FileStream(_input("options", "convertable", ".json"))
        .filter(
            lambda x: (
                (
                    Stream(x.value)
                    .find_first(lambda y: y.key == "name" and y.value == "Snake")
                    .or_else_get(lambda: None)
                )
                is None
            )
        )
        .map(lambda x: x.value)
        .save(tmp_file_path)
    )
    assert tmp_file_path.read_text() == (EXPECTED / "convert" / "converted.csv").read_text()


def test_save_to_csv_with_null_handler(tmp_file_dir):
    def _null_handler(dict_obj):
        return Stream(dict_obj).to_dict(lambda x: DictItem(x.key, x.value or "N/A"))

    tmp_file_path = tmp_file_dir / "converted_null.csv"
    (
        FileStream(_input("options", "convertable", ".json"))
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
    assert tmp_file_path.read_text() == (EXPECTED / "convert" / "converted_null.csv").read_text()


def test_save_empty_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "dead.csv"
    shutil.copyfile(INPUT / "dsv" / "bar.csv", tmp_file_path)
    stream = FileStream(tmp_file_path)
    stream._iterable = tuple()
    stream.save()
    assert tmp_file_path.read_text() == (EXPECTED / "update" / "empty.csv").read_text()


def test_update_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "updated.csv"
    shutil.copyfile(INPUT / "dsv" / "editable.csv", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .map(lambda x: Stream(x).to_dict(lambda y: DictItem(y.key, y.value or "Unknown")))
        .save(tmp_file_path)
    )
    assert tmp_file_path.read_text() == (EXPECTED / "update" / "updated.csv").read_text()


def test_update_fails(tmp_file_dir):
    err_msg = "Ooops Mr. White..."

    def _err_raiser(_):
        raise IOError(err_msg)

    tmp_file_path = tmp_file_dir / "fail.csv"
    shutil.copyfile(INPUT / "dsv" / "editable.csv", tmp_file_path)
    with pytest.raises(IOError, match=err_msg):
        FileStream(tmp_file_path).save(tmp_file_path, null_handler=_err_raiser)
    assert tmp_file_path.read_text() == (INPUT / "dsv" / "editable.csv").read_text()


def test_combine_files_into_csv(tmp_file_dir):
    tmp_file_path = tmp_file_dir / "merged.csv"
    shutil.copyfile(INPUT / "dsv" / "combine.csv", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .concat(
            FileStream(_input("options", "convertable", ".json"))
            .filter(
                lambda x: (
                    (
                        Stream(x.value)
                        .find_first(lambda y: y.key == "name" and y.value != "Snake")
                        .or_else_get(lambda: None)
                    )
                    is not None
                )
            )
            .map(lambda x: x.value)
        )
        .map(lambda x: Stream(x).to_dict(lambda y: DictItem(y.key, y.value or "N/A")))
        .save(tmp_file_path)
    )
    assert tmp_file_path.read_text() == (EXPECTED / "convert" / "merged.csv").read_text()


@pytest.mark.parametrize("suffix", [".json", ".yaml"])
def test_convert_csv_to_mapping(tmp_file_dir, suffix):
    tmp_file_path = tmp_file_dir / f"bar{suffix}"
    FileStream(str(INPUT / "dsv" / "bar.csv")).map(lambda r: DictItem(r["fizz"], {"buzz": r["buzz"]})).save(
        tmp_file_path, f_write={"indent": 2}
    )
    assert tmp_file_path.read_text() == (EXPECTED / "convert" / f"bar{suffix}").read_text()


def test_save_mapping_to_plain(tmp_file_dir, json_dict):
    in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
    file_path = "dict_2_plain.txt"
    tmp_file_path = tmp_file_dir / file_path
    FileStream(_input("nested", "nested", ".json")).prepend(in_memory_dict).map(
        lambda x: f"{x._key}: {x._value}"
    ).save(tmp_file_path)
    assert tmp_file_path.read_text() == (EXPECTED / "plain" / file_path).read_text()


def test_append_to_plain(tmp_file_dir, json_dict):
    file_path = "append_map.txt"
    tmp_file_path = tmp_file_dir / file_path
    shutil.copyfile(INPUT / "plain" / "plain_dict.txt", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .map(lambda line: line.strip())
        .enumerate()
        .filter(lambda line: "ne" in line[1])
        .map(lambda line: f"line_num:{line[0]}, text='{line[1]}'")
        .save(f_open={"mode": "a"})
    )
    assert tmp_file_path.read_text() == (EXPECTED / "plain" / file_path).read_text()


def test_plain_text_header_footer(tmp_file_dir):
    file_path = "foo.txt"
    tmp_file_path = tmp_file_dir / file_path
    shutil.copyfile(INPUT / "plain" / "plain.txt", tmp_file_path)
    (
        FileStream(tmp_file_path)
        .map(lambda line: line.strip())
        .enumerate()
        .filter(lambda line: line[0] == 3)
        .map(lambda line: f"{line[0]}: {line[1]}")
        .save(
            f_open={"mode": "a"},
            f_write={"header": "\nHeader\n", "footer": "\nFooter\n"},
        )
    )
    assert tmp_file_path.read_text() == (EXPECTED / "plain" / file_path).read_text()


@pytest.mark.parametrize("suffix", [".json", ".yaml", ".toml"])
def test_read_empty_mapping(suffix):
    assert FileStream(_input("options", "empty", suffix)).to_list() == []


def test_read_empty_xml_raises():
    with pytest.raises(NoneTypeError):
        FileStream(_input("options", "empty", ".xml")).to_list()


def test_read_empty_csv():
    assert FileStream(_input("options", "empty", ".csv")).to_list() == []


def test_read_empty_plain():
    assert FileStream(str(INPUT / "plain" / "empty.txt")).to_list() == []


def test_save_empty_json(tmp_file_dir):
    tmp = tmp_file_dir / "empty.json"
    FileStream(_input("options", "empty", ".json")).save(tmp)
    assert tmp.read_text() == (EXPECTED / "save" / "empty.json").read_text()


@pytest.mark.parametrize(
    "suffix, keys",
    [
        (".json", ["名前", "café"]),
        (".yaml", ["名前", "café"]),
        (".toml", ["名前", "café"]),
        (".xml", ["name", "cafe"]),
    ],
)
def test_read_unicode(suffix, keys):
    assert FileStream(_input("options", "unicode", suffix)).map(lambda x: x.key).to_list() == keys


@pytest.mark.parametrize(
    "suffix, exc",
    [
        (".json", JSONDecodeError),
        (".yaml", ParserError),
        (".xml", ExpatError),
        (".toml", TOMLDecodeError),
        (".ini", MissingSectionHeaderError),
    ],
)
def test_malformed_raises(suffix, exc):
    with pytest.raises(exc):
        FileStream(_input("options", "malformed", suffix)).to_list()


def test_dsv_custom_delimiter(tmp_file_dir):
    src = INPUT / "dsv" / "bar.csv"
    tmp = tmp_file_dir / "piped.csv"
    FileStream(str(src)).save(tmp, f_write={"delimiter": "|"})
    assert FileStream.process(tmp, f_read={"delimiter": "|"}).map(
        lambda x: f"{x['fizz']}:{x['buzz']}"
    ).to_list() == ["42:45", "aaa:bbb"]


def test_file_handler_closed_on_exception(monkeypatch):
    import builtins

    from pyrio.streams import BaseStream

    close_called = []
    original_init = BaseStream.__init__
    original_open = builtins.open

    def mock_init(self, iterable):
        original_init(self, iterable)
        raise RuntimeError("Simulated initialization error")

    def tracking_open(*args, **kwargs):
        f = original_open(*args, **kwargs)
        original_close = f.close

        def tracked_close():
            close_called.append(True)
            return original_close()

        f.close = tracked_close
        return f

    monkeypatch.setattr(BaseStream, "__init__", mock_init)
    monkeypatch.setattr(builtins, "open", tracking_open)

    with pytest.raises(RuntimeError, match="Simulated initialization error"):
        FileStream(_input("flat", "foo", ".json"))

    assert len(close_called) > 0, "File handler was not closed after exception"


def test_save_with_cleaning_up_tmp_file(tmp_file_dir):
    source_path = tmp_file_dir / "stale_tmp_source.json"
    shutil.copyfile(_input("flat", "foo", ".json"), source_path)

    tmp_path = Path(f"{source_path}.tmp")
    tmp_path.write_text("stale tmp content")
    assert tmp_path.exists()

    FileStream(source_path).save()
    assert not tmp_path.exists()


def test_atomic_write_cleanup_on_serialization_error(tmp_file_dir):
    class NotSerializable:
        pass

    source_path = tmp_file_dir / "serialize_fail.json"
    shutil.copyfile(_input("flat", "foo", ".json"), source_path)

    fs = FileStream(source_path)
    fs._iterable = (DictItem("key", NotSerializable()),)

    with pytest.raises(TypeError):
        fs.save()

    tmp_path = Path(f"{source_path}.tmp")
    assert not tmp_path.exists()
    assert source_path.read_text() == Path(_input("flat", "foo", ".json")).read_text()


def test_pickle_dict_round_trip(tmp_file_dir):
    src = tmp_file_dir / "dict.pickle"
    data = {"abc": "xyz", "qwerty": 42}
    src.write_bytes(pickle.dumps(data))
    assert FileStream(src).to_dict() == data

    out = tmp_file_dir / "dict_out.pickle"
    FileStream(src).save(out)
    assert FileStream(out).to_dict() == data


@pytest.mark.parametrize(
    "data",
    [{}, {"nested": {"x": 1}}, [], [1, 2], (1, 2), "xyz", 42, None],
)
def test_pickle_load(tmp_file_dir, data):
    src = tmp_file_dir / "data.pickle"
    src.write_bytes(pickle.dumps(data))
    loaded = FileStream(src)
    match data:
        case dict():
            assert loaded.to_dict() == data
        case list():
            assert loaded.to_list() == data
        case tuple():
            assert loaded.to_list() == list(data)
        case _:
            assert loaded.to_list() == [data]


def test_pickle_inplace_null_and_protocol(tmp_file_dir):
    path = tmp_file_dir / "data.pickle"
    path.write_bytes(pickle.dumps({"a": 1, "b": None}))
    FileStream(path).save(
        null_handler=lambda x: DictItem(x.key, "N/A") if x.value is None else x,
        f_write={"protocol": pickle.HIGHEST_PROTOCOL},
    )
    assert FileStream(path).to_dict() == {"a": 1, "b": "N/A"}


def test_pickle_malformed_raises(tmp_file_dir):
    src = tmp_file_dir / "bad.pickle"
    src.write_bytes(b"not-a-pickle")
    with pytest.raises(pickle.UnpicklingError):
        FileStream(src).to_list()


@pytest.mark.parametrize(
    "build, materialize, payload",
    [
        (
            lambda: FileStream(_input("flat", "foo", ".json")).map(lambda x: (x.key, x.value)),
            "list",
            [("abc", "xyz"), ("qwerty", 42)],
        ),
        (
            lambda: FileStream(_input("flat", "foo", ".json")).map(lambda x: x.key),
            "tuple",
            ("abc", "qwerty"),
        ),
        (
            lambda: (
                FileStream(_input("flat", "foo", ".json"))
                .filter(lambda x: x.key == "abc")
                .map(lambda x: x.value)
            ),
            "raw",
            "xyz",
        ),
        (
            lambda: FileStream(_input("flat", "foo", ".json")),
            lambda s: {item.key.upper(): item.value for item in s.iterable},
            {"ABC": "xyz", "QWERTY": 42},
        ),
    ],
)
def test_pickle_materialize(tmp_file_dir, build, materialize, payload):
    out = tmp_file_dir / "out.pickle"
    build().save(out, materialize=materialize)
    assert pickle.loads(out.read_bytes()) == payload

    loaded = FileStream(out)
    match payload:
        case dict():
            assert loaded.to_dict() == payload
        case list():
            assert loaded.to_list() == payload
        case tuple():
            assert loaded.to_list() == list(payload)
        case _:
            assert loaded.to_list() == [payload]  # wrap_scalars


@pytest.mark.parametrize(
    "materialize, match",
    [
        ("raw", "materialize='raw' requires exactly 1 element"),
        ("set", "Unsupported materialize"),
    ],
)
def test_materialize_errors(tmp_file_dir, materialize, match):
    with pytest.raises(ValueError, match=match):
        FileStream(_input("flat", "foo", ".json")).save(tmp_file_dir / "bad.pickle", materialize=materialize)


def test_json_materialize_list(tmp_file_dir):
    out = tmp_file_dir / "list.json"
    FileStream(_input("flat", "foo", ".json")).map(lambda x: x.key).save(out, materialize="list")
    assert json.loads(out.read_text()) == ["abc", "qwerty"]


def _input(subdir, name, suffix):
    return str(INPUT / subdir / f"{name}{suffix}")

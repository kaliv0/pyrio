import shutil
from decimal import Decimal
from operator import attrgetter

import pytest

from pyrio import (
    FileStream,
    Stream,
    DictItem,
    FileOptions,
    CsvReadOptions,
    CsvWriteOptions,
    JsonReadOptions,
    JsonWriteOptions,
    YamlWriteOptions,
    XmlWriteOptions,
    PlainTextWriteOptions,
)
from pyrio.exceptions import IllegalStateError, NoneTypeError


class TestFileStream:
    def test_none_path_error(self):
        with pytest.raises(NoneTypeError) as e:
            FileStream(None)
        assert str(e.value) == "File path cannot be None"

    def test_invalid_path_error(self):
        file_path = "./foo/bar.xyz"
        with pytest.raises(FileNotFoundError) as e:
            FileStream(file_path)
        assert str(e.value) == f"No such file or directory: '{file_path}'"

    def test_path_is_dir_error(self):
        file_path = "./tests/resources/"
        with pytest.raises(IsADirectoryError) as e:
            FileStream(file_path)
        assert str(e.value) == f"Given path '{file_path}' is a directory"

    @pytest.mark.parametrize(
        "file_path",
        [
            "./tests/resources/foo.json",
            "./tests/resources/foo.toml",
            "./tests/resources/foo.yaml",
            "./tests/resources/foo.yml",
            "./tests/resources/foo.xml",
        ],
    )
    def test_read_files(self, file_path):
        assert FileStream(file_path).map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
            "abc=>xyz",
            "qwerty=>42",
        )

    def test_read_xml_custom_root(self):
        assert FileStream("./tests/resources/custom_root.xml").map(
            lambda x: f"{x.key}=>{x.value}"
        ).to_tuple() == (
            "abc=>xyz",
            "qwerty=>42",
        )

    def test_read_xml_include_root(self):
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
    def test_dsv(self, file_path):
        assert FileStream(file_path).map(
            lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}"
        ).to_tuple() == (
            "fizz: 42, buzz: 45",
            "fizz: aaa, buzz: bbb",
        )

    def test_read_plain_text(self):
        lorem = FileStream("./tests/resources/plain.txt")
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

    def test_read_plain_and_query(self):
        assert FileStream("./tests/resources/plain.txt").map(
            lambda x: x.strip()
        ).enumerate().filter(lambda line: "id" in line[1]).to_dict() == {
            1: "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            6: "Excepteur sint occaecat cupidatat non proident, sunt in culpa",
            7: "qui officia deserunt mollit anim id est laborum.",
        }

    def test_nested_json(self):
        assert FileStream("./tests/resources/nested.json").map(lambda x: x.value).flat_map(
            lambda x: Stream(x)
            .filter(lambda y: y.key == "second")
            .flat_map(lambda z: z.value)
            .to_tuple()
        ).to_list() == [
            1,
            2,
            3,
            4,
        ]

    def test_nested_dict(self):
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

    def test_complex_pipeline(self):
        assert (
            FileStream("./tests/resources/long.json")
            .filter(lambda x: "a" in x.key)
            .map(lambda x: DictItem(x.key, sum(x.value) * 10))
            .sort(attrgetter("value"), reverse=True)
            .map(lambda x: f"{str(x.value)}::{x.key}")
        ).to_list() == ["230::xza", "110::abba", "30::a"]

    def test_reusing_stream(self):
        stream = FileStream("./tests/resources/foo.json")
        assert stream._is_consumed is False

        result = stream.map(lambda x: f"{x.key}=>{x.value}").tail(1).to_tuple()
        assert result == ("qwerty=>42",)
        assert stream._is_consumed
        assert stream._file_handler.closed

        with pytest.raises(IllegalStateError) as e:
            stream.map(lambda x: x.value * 10).to_list()
        assert str(e.value) == "Stream object already consumed"

    def test_concat(self):
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

    def test_prepend(self, json_dict):
        in_memory_dict = Stream(json_dict).to_tuple()
        assert (
            FileStream("./tests/resources/long.json")
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

    @pytest.mark.parametrize(
        "file_path",
        ["./tests/resources/parse_float.toml", "./tests/resources/parse_float.json"],
    )
    def test_process(self, file_path):
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

        assert FileStream.process(file_path, f_read_options={"parse_float": Decimal}).all_match(
            check_type
        )

    # ### save to file ###
    def test_save_toml(self, tmp_file_dir, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / "test.toml"
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/test.toml").read()

    def test_save_toml_default_null_handler(self, tmp_file_dir, json_dict):
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
    def test_save(self, tmp_file_dir, file_path, indent, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / file_path
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_open_options={"encoding": "utf-8"},
            f_write_options={"indent": indent},
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    @pytest.mark.parametrize(
        "file_path, indent",
        [
            ("test_null_handler.json", 2),
            ("test_null_handler.yaml", 2),
            ("test_null_handler.yml", 2),
            ("test_null_handler.xml", 4),
        ],
    )
    def test_save_handle_null(self, tmp_file_dir, file_path, indent, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / file_path
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_open_options={"encoding": "utf-8"},
            f_write_options={"indent": indent},
            null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_save_custom_xml_root(self, tmp_file_dir, json_dict):
        file_path = "custom_root.xml"
        tmp_file_path = tmp_file_dir / file_path
        indent = 4

        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_write_options={"indent": indent},
            null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
            xml_root="my-root",
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_save_plain(self, tmp_file_dir):
        file_path = "lorem.txt"
        tmp_file_path = tmp_file_dir / "lorem.txt"
        fs = FileStream("./tests/resources/plain.txt")
        (
            fs.map(lambda line: line.strip())
            .enumerate()
            .filter(lambda line: "x" in line[1])
            .map(lambda line: f"line_num:{line[0]}, text='{line[1]}'")
            .save(tmp_file_path)
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )
        assert fs._file_handler.closed

    def test_save_raises(self):
        with pytest.raises(UnicodeDecodeError) as e:
            FileStream("./tests/resources/awake.mp3").save("./tests/resources/woke.json")
        assert (
            str(e.value)
            == "'utf-8' codec can't decode byte 0xff in position 45: invalid start byte"
        )

    def test_update_plain(self, tmp_file_dir, json_dict):
        file_path = "lorem.txt"
        tmp_file_path = tmp_file_dir / file_path
        shutil.copyfile("./tests/resources/plain.txt", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .map(lambda line: line.strip())
            .enumerate()
            .filter(lambda line: "x" in line[1])
            .map(lambda line: f"line_num:{line[0]}, text='{line[1]}'")
            .save()
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_update_file(self, tmp_file_dir, json_dict):
        tmp_file_path = tmp_file_dir / "updated.json"
        shutil.copyfile("./tests/resources/long.json", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .map(
                lambda x: DictItem(
                    x.key, ", ".join((str(y) for y in x.value)) if x.value else x.value
                )
            )
            .save(
                f_write_options={"indent": 2},
                null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
            )
        )
        assert (
            tmp_file_path.read_text() == open("./tests/resources/save_output/updated.json").read()
        )

    def test_filter_update_file(self, tmp_file_dir, json_dict):
        file_path = "filtered.toml"
        tmp_file_path = tmp_file_dir / file_path
        shutil.copyfile("./tests/resources/test.toml", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .filter(lambda x: isinstance(x.value, str))
            .reverse(comparator=lambda x: x.key)
            .save()
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    @pytest.mark.parametrize(
        "file_path",
        ["test.csv", "test.tsv"],
    )
    def test_save_csv(self, tmp_file_dir, file_path):
        tmp_file_path = tmp_file_dir / file_path
        FileStream("./tests/resources/bar.csv").save(tmp_file_path)
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_save_convert_to_csv(self, tmp_file_dir):
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
            tmp_file_path.read_text() == open("./tests/resources/save_output/converted.csv").read()
        )

    def test_save_to_csv_with_null_handler(self, tmp_file_dir):
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
            tmp_file_path.read_text()
            == open("./tests/resources/save_output/converted_null.csv").read()
        )

    def test_save_empty_csv(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "dead.csv"
        shutil.copyfile("./tests/resources/bar.csv", tmp_file_path)
        stream = FileStream(tmp_file_path)
        stream._iterable = tuple()
        stream.save()
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/empty.csv").read()

    def test_update_csv(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "updated.csv"
        shutil.copyfile("./tests/resources/editable.csv", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .map(lambda x: (Stream(x).to_dict(lambda y: DictItem(y.key, y.value or "Unknown"))))
            .save(tmp_file_path)
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/updated.csv").read()

    def test_update_fails(self, tmp_file_dir):
        def _raise(exception):
            raise exception

        tmp_file_path = tmp_file_dir / "fail.csv"
        shutil.copyfile("./tests/resources/editable.csv", tmp_file_path)
        with pytest.raises(IOError, match="Ooops Mr White..."):
            FileStream(tmp_file_path).save(
                tmp_file_path, null_handler=_raise(IOError("Ooops Mr White..."))
            )
        assert tmp_file_path.read_text() == open("./tests/resources/editable.csv").read()

    def test_combine_files_into_csv(self, tmp_file_dir):
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
                    is not None  # explicit is better than implicit
                )
                .map(lambda x: x.value)
            )
            .map(lambda x: (Stream(x).to_dict(lambda y: DictItem(y.key, y.value or "N/A"))))
            .save(tmp_file_path)
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/merged.csv").read()

    def test_save_mapping_to_plain(self, tmp_file_dir, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        file_path = "dict_2_plain.txt"
        tmp_file_path = tmp_file_dir / file_path
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).map(
            lambda x: f"{x._key}: {x._value}"
        ).save(
            tmp_file_path,
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_append_to_plain(self, tmp_file_dir, json_dict):
        file_path = "append_map.txt"
        tmp_file_path = tmp_file_dir / file_path
        shutil.copyfile("./tests/resources/plain_dict.txt", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .map(lambda line: line.strip())
            .enumerate()
            .filter(lambda line: "ne" in line[1])
            .map(lambda line: f"line_num:{line[0]}, text='{line[1]}'")
            .save(f_open_options={"mode": "a"})
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_plain_text_header_footer(self, tmp_file_dir):
        file_path = "foo.txt"
        tmp_file_path = tmp_file_dir / file_path
        shutil.copyfile("./tests/resources/plain.txt", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .map(lambda line: line.strip())
            .enumerate()
            .filter(lambda line: line[0] == 3)
            .map(lambda line: f"{line[0]}: {line[1]}")
            .save(
                f_open_options={"mode": "a"},
                f_write_options={"header": "\nHeader\n", "footer": "\nFooter\n"},
            )
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_file_handler_closed_on_exception(self, monkeypatch):
        from pyrio.streams import BaseStream
        import builtins

        # Track if close was called
        close_called = []
        original_init = BaseStream.__init__
        original_open = builtins.open

        def mock_init(self, iterable):
            # set up _iterable
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
            FileStream("./tests/resources/foo.json")

        assert len(close_called) > 0, "File handler was not closed after exception"

    def test_save_with_cleaning_up_tmp_file(self, tmp_file_dir):
        from pathlib import Path

        source_path = tmp_file_dir / "stale_tmp_source.json"
        shutil.copyfile("./tests/resources/foo.json", source_path)

        tmp_path = Path(f"{source_path}.tmp")
        tmp_path.write_text("stale tmp content")
        assert tmp_path.exists()

        FileStream(source_path).save()
        assert not tmp_path.exists()

    def test_atomic_write_cleanup_on_serialization_error(self, tmp_file_dir):
        from pathlib import Path

        class NotSerializable:
            pass

        source_path = tmp_file_dir / "serialize_fail.json"
        shutil.copyfile("./tests/resources/foo.json", source_path)

        fs = FileStream(source_path)
        fs._iterable = (DictItem("key", NotSerializable()),)

        with pytest.raises(TypeError):
            fs.save()

        # Tmp file should be cleaned up
        tmp_path = Path(f"{source_path}.tmp")
        assert not tmp_path.exists()

        # Source file should be unchanged
        assert source_path.read_text() == open("./tests/resources/foo.json").read()

    # ### file_options ###
    # JSON options
    def test_read_json_with_options(self):
        result = FileStream.process(
            "./tests/resources/parse_float.json", f_read_options=JsonReadOptions.with_decimal()
        ).to_tuple()
        assert any(isinstance(item.value, Decimal) for item in result if hasattr(item, "value"))

    def test_save_json_with_json_write_options(self, tmp_file_dir, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / "test_opts.json"
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_open_options=FileOptions.utf8(),
            f_write_options=JsonWriteOptions.pretty(indent=2),
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/test.json").read()

    def test_save_json_with_sorted_options(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "sorted.json"
        FileStream("./tests/resources/foo.json").save(
            tmp_file_path,
            f_write_options=JsonWriteOptions.sorted(indent=2),
        )
        content = tmp_file_path.read_text()
        # With sort_keys=True, "abc" should come before "qwerty"
        assert content.index("abc") < content.index("qwerty")

    def test_save_json_with_compact_options(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "compact.json"
        FileStream("./tests/resources/foo.json").save(
            tmp_file_path,
            f_write_options=JsonWriteOptions.compact(),
        )
        content = tmp_file_path.read_text()
        # Compact format has no spaces after colons/commas
        assert '": "' not in content
        assert '":"' in content

    def test_update_json_with_options(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "updated_opts.json"
        shutil.copyfile("./tests/resources/long.json", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .map(
                lambda x: DictItem(
                    x.key, ", ".join((str(y) for y in x.value)) if x.value else x.value
                )
            )
            .save(
                f_write_options=JsonWriteOptions(indent=2),
                null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
            )
        )
        assert (
            tmp_file_path.read_text() == open("./tests/resources/save_output/updated.json").read()
        )

    # CSV options
    def test_read_csv_with_options(self):
        result = FileStream.process(
            "./tests/resources/bar.csv",
            f_read_options=CsvReadOptions(delimiter=","),
        ).to_tuple()
        assert len(result) == 2
        assert result[0]["fizz"] == "42"

    def test_read_csv_with_excel_dialect(self):
        result = FileStream.process(
            "./tests/resources/bar.csv",
            f_read_options=CsvReadOptions.excel(),
        ).to_tuple()
        assert len(result) == 2

    def test_save_csv_with_options(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "test_opts.csv"
        FileStream("./tests/resources/bar.csv").save(
            tmp_file_path,
            f_write_options=CsvWriteOptions(delimiter=","),
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/test.csv").read()

    def test_save_csv_with_custom_delimiter(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "semicolon.csv"
        FileStream("./tests/resources/bar.csv").save(
            tmp_file_path,
            f_write_options=CsvWriteOptions(delimiter=";"),
        )
        content = tmp_file_path.read_text()
        assert ";" in content
        assert "fizz;buzz" in content

    def test_read_csv_with_custom_quotechar(self, tmp_file_dir):
        # Create a CSV with single quotes
        tmp_file_path = tmp_file_dir / "quoted.csv"
        tmp_file_path.write_text("name,value\n'hello','world'\n")
        result = FileStream.process(
            tmp_file_path,
            f_read_options=CsvReadOptions(quotechar="'"),
        ).to_tuple()
        assert len(result) == 1

    # YAML options
    def test_save_yaml_with_options(self, tmp_file_dir, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / "test_opts.yaml"
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_open_options=FileOptions(encoding="utf-8"),
            f_write_options=YamlWriteOptions(indent=2),
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/test.yaml").read()

    def test_save_yaml_with_block_style(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "block.yaml"
        FileStream("./tests/resources/foo.yaml").save(
            tmp_file_path,
            f_write_options=YamlWriteOptions.block_style(indent=2),
        )
        assert tmp_file_path.read_text() == "abc: xyz\nqwerty: 42\n"

    def test_save_yaml_with_flow_style(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "flow.yaml"
        FileStream("./tests/resources/foo.yaml").save(
            tmp_file_path,
            f_write_options=YamlWriteOptions.flow_style(),
        )
        assert tmp_file_path.read_text() == "{abc: xyz, qwerty: 42}\n"

    # XML options
    def test_save_xml_with_options(self, tmp_file_dir, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / "test_opts.xml"
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_open_options=FileOptions.utf8(),
            f_write_options=XmlWriteOptions(pretty=True, indent=4),
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/test.xml").read()

    def test_save_xml_with_pretty_print_factory(self, tmp_file_dir, json_dict):
        file_path = "custom_root.xml"
        tmp_file_path = tmp_file_dir / file_path
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_write_options=XmlWriteOptions.pretty_print(indent=4),
            null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
            xml_root="my-root",
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    # Plain text options
    def test_save_plain_with_options(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "test_opts.txt"
        FileStream("./tests/resources/plain.txt").map(lambda x: x.strip()).head(2).save(
            tmp_file_path,
            f_write_options=PlainTextWriteOptions.with_header_footer(
                header="---START---\n", footer="\n---END---"
            ),
        )
        assert tmp_file_path.read_text() == (
            "---START---\n"
            "Lorem ipsum dolor sit amet, consectetur adipisicing elit,\n"
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n"
            "---END---"
        )

    # FileOptions tests
    def test_append_to_plain_with_file_options(self, tmp_file_dir):
        file_path = "append_map.txt"
        tmp_file_path = tmp_file_dir / file_path
        shutil.copyfile("./tests/resources/plain_dict.txt", tmp_file_path)
        (
            FileStream(tmp_file_path)
            .map(lambda line: line.strip())
            .enumerate()
            .filter(lambda line: "ne" in line[1])
            .map(lambda line: f"line_num:{line[0]}, text='{line[1]}'")
            .save(f_open_options=FileOptions.append())
        )
        assert (
            tmp_file_path.read_text() == open(f"./tests/resources/save_output/{file_path}").read()
        )

    def test_save_with_utf8_file_options(self, tmp_file_dir, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / "utf8_opts.json"
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_open_options=FileOptions.utf8(),
            f_write_options=JsonWriteOptions(indent=2),
        )
        assert tmp_file_path.read_text() == open("./tests/resources/save_output/test.json").read()

    def test_save_with_ascii_file_options(self, tmp_file_dir):
        tmp_file_path = tmp_file_dir / "ascii.json"
        FileStream("./tests/resources/foo.json").save(
            tmp_file_path,
            f_open_options=FileOptions.ascii(),
            f_write_options=JsonWriteOptions(indent=2, ensure_ascii=True),
        )
        content = tmp_file_path.read_text()
        assert "abc" in content

    # Combined options tests
    def test_save_with_combined_options(self, tmp_file_dir, json_dict):
        in_memory_dict = Stream(json_dict).filter(lambda x: len(x.key) < 6).to_tuple()
        tmp_file_path = tmp_file_dir / "combined.json"
        FileStream("./tests/resources/nested.json").prepend(in_memory_dict).save(
            tmp_file_path,
            f_open_options=FileOptions(encoding="utf-8"),
            f_write_options=JsonWriteOptions(indent=2),
            null_handler=lambda x: DictItem(x.key, "Unknown") if x.value is None else x,
        )
        assert (
            tmp_file_path.read_text()
            == open("./tests/resources/save_output/test_null_handler.json").read()
        )

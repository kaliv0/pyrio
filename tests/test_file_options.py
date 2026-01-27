from decimal import Decimal

import yaml

from pyrio import (
    FileStream,
    FileOpts,
    CsvReadOpts,
    CsvWriteOpts,
    JsonReadOpts,
    JsonWriteOpts,
    YamlReadOpts,
    YamlWriteOpts,
    XmlReadOpts,
    XmlWriteOpts,
    TextWriteOpts,
)


class TestFileOptions:
    def test_init_with_all_params(self):
        opts = FileOpts(encoding="utf-8", errors="strict", newline="\n", buffering=1, mode="w")
        assert opts.encoding == "utf-8"
        assert opts.errors == "strict"
        assert opts.newline == "\n"
        assert opts.buffering == 1
        assert opts.mode == "w"

    def test_to_dict_all_values(self):
        opts = FileOpts(encoding="utf-8", errors="ignore", newline="", buffering=0, mode="w")
        assert opts.to_dict() == {
            "encoding": "utf-8",
            "errors": "ignore",
            "newline": "",
            "buffering": 0,
            "mode": "w",
        }

    def test_to_dict_partial_values(self):
        assert FileOpts(encoding="latin-1").to_dict() == {"encoding": "latin-1"}

    def test_to_dict_empty(self):
        assert FileOpts().to_dict() == {}

    def test_utf8_factory(self):
        opts = FileOpts.utf8()
        assert opts.encoding == "utf-8"
        assert opts.to_dict() == {"encoding": "utf-8"}

    def test_utf8_factory_with_errors(self):
        opts = FileOpts.utf8(errors="replace")
        assert opts.encoding == "utf-8"
        assert opts.errors == "replace"
        assert opts.to_dict() == {"encoding": "utf-8", "errors": "replace"}

    def test_ascii_factory(self):
        opts = FileOpts.ascii()
        assert opts.encoding == "ascii"
        assert opts.to_dict() == {"encoding": "ascii"}

    def test_repr(self):
        assert str(FileOpts(encoding="utf-8")) == "FileOpts(encoding='utf-8')"


class TestCsvReadOptions:
    def test_init_with_params(self):
        opts = CsvReadOpts(delimiter=";", quotechar="'", strict=True)
        assert opts.delimiter == ";"
        assert opts.quotechar == "'"
        assert opts.strict is True

    def test_to_dict(self):
        assert CsvReadOpts(delimiter="|", skipinitialspace=True).to_dict() == {
            "delimiter": "|",
            "skipinitialspace": True,
        }

    def test_excel_factory(self):
        opts = CsvReadOpts.excel()
        assert opts.dialect == "excel"
        assert opts.to_dict() == {"dialect": "excel"}

    def test_unix_factory(self):
        opts = CsvReadOpts.unix()
        assert opts.dialect == "unix"
        assert opts.to_dict() == {"dialect": "unix"}

    def test_repr(self):
        assert str(CsvReadOpts(delimiter=";")) == "CsvReadOpts(delimiter=';')"


class TestCsvWriteOptions:
    def test_init_with_params(self):
        opts = CsvWriteOpts(delimiter="\t", lineterminator="\r\n")
        assert opts.delimiter == "\t"
        assert opts.lineterminator == "\r\n"

    def test_to_dict(self):
        assert CsvWriteOpts(delimiter=";", extrasaction="ignore").to_dict() == {
            "delimiter": ";",
            "extrasaction": "ignore",
        }

    def test_repr(self):
        assert str(CsvWriteOpts(delimiter="|")) == "CsvWriteOpts(delimiter='|')"


class TestJsonReadOptions:
    def test_init_with_params(self):
        assert JsonReadOpts(parse_float=Decimal).parse_float == Decimal

    def test_to_dict(self):
        assert JsonReadOpts(parse_float=Decimal, parse_int=str).to_dict() == {
            "parse_float": Decimal,
            "parse_int": str,
        }

    def test_with_decimal_factory(self):
        opts = JsonReadOpts.with_decimal()
        assert opts.parse_float == Decimal
        assert opts.to_dict() == {"parse_float": Decimal}

    def test_repr(self):
        assert str(JsonReadOpts(parse_float=float)) == "JsonReadOpts(parse_float=<class 'float'>)"


class TestJsonWriteOptions:
    def test_init_with_params(self):
        opts = JsonWriteOpts(indent=4, sort_keys=True)
        assert opts.indent == 4
        assert opts.sort_keys is True

    def test_to_dict(self):
        assert JsonWriteOpts(indent=2, ensure_ascii=False).to_dict() == {
            "indent": 2,
            "ensure_ascii": False,
        }

    def test_pretty_factory(self):
        opts = JsonWriteOpts.pretty()
        assert opts.indent == 2
        assert opts.to_dict() == {"indent": 2}

    def test_pretty_factory_custom_indent(self):
        opts = JsonWriteOpts.pretty(indent=4)
        assert opts.indent == 4

    def test_compact_factory(self):
        opts = JsonWriteOpts.compact()
        assert opts.separators == (",", ":")
        assert opts.to_dict() == {"separators": (",", ":")}

    def test_sorted_factory(self):
        opts = JsonWriteOpts.sorted()
        assert opts.indent == 2
        assert opts.sort_keys is True
        assert opts.to_dict() == {"indent": 2, "sort_keys": True}

    def test_repr(self):
        assert str(JsonWriteOpts(indent=2)) == "JsonWriteOpts(indent=2)"


class TestYamlReadOptions:
    def test_init_with_params(self):
        opts = YamlReadOpts(loader=yaml.SafeLoader)
        assert opts.loader == yaml.SafeLoader

    def test_to_dict(self):
        assert YamlReadOpts(loader=yaml.FullLoader).to_dict() == {"loader": yaml.FullLoader}

    def test_to_dict_empty(self):
        assert YamlReadOpts().to_dict() == {}

    def test_repr(self):
        assert (
            str(YamlReadOpts(loader=yaml.FullLoader))
            == "YamlReadOpts(loader=<class 'yaml.loader.FullLoader'>)"
        )


class TestYamlWriteOptions:
    def test_init_with_params(self):
        opts = YamlWriteOpts(default_flow_style=False, indent=4)
        assert opts.default_flow_style is False
        assert opts.indent == 4

    def test_to_dict(self):
        assert YamlWriteOpts(allow_unicode=True, width=80).to_dict() == {
            "allow_unicode": True,
            "width": 80,
        }

    def test_block_style_factory(self):
        opts = YamlWriteOpts.block_style()
        assert opts.default_flow_style is False
        assert opts.indent == 2

    def test_block_style_factory_custom_indent(self):
        opts = YamlWriteOpts.block_style(indent=4)
        assert opts.indent == 4

    def test_flow_style_factory(self):
        opts = YamlWriteOpts.flow_style()
        assert opts.default_flow_style is True

    def test_repr(self):
        assert str(YamlWriteOpts(indent=2)) == "YamlWriteOpts(indent=2)"


class TestXmlReadOptions:
    def test_init_with_params(self):
        opts = XmlReadOpts(attr_prefix="@", cdata_key="#text")
        assert opts.attr_prefix == "@"
        assert opts.cdata_key == "#text"

    def test_to_dict(self):
        assert XmlReadOpts(process_namespaces=True).to_dict() == {"process_namespaces": True}

    def test_repr(self):
        assert (
            str(XmlReadOpts(attr_prefix="@", cdata_key="#text", process_namespaces=True))
            == "XmlReadOpts(process_namespaces=True, attr_prefix='@', cdata_key='#text')"
        )


class TestXmlWriteOptions:
    def test_init_with_params(self):
        opts = XmlWriteOpts(pretty=True, indent=4)
        assert opts.pretty is True
        assert opts.indent == 4

    def test_to_dict(self):
        assert XmlWriteOpts(short_empty_elements=True).to_dict() == {"short_empty_elements": True}

    def test_pretty_print_factory(self):
        opts = XmlWriteOpts.pretty()
        assert opts.pretty is True
        assert opts.indent == 4

    def test_pretty_print_factory_custom_indent(self):
        opts = XmlWriteOpts.pretty(indent=2)
        assert opts.indent == 2

    def test_repr(self):
        assert str(XmlWriteOpts(pretty=True)) == "XmlWriteOpts(pretty=True)"


class TestPlainTextWriteOptions:
    def test_init_with_params(self):
        opts = TextWriteOpts(delimiter=", ", header="START", footer="END")
        assert opts.delimiter == ", "
        assert opts.header == "START"
        assert opts.footer == "END"

    def test_to_dict(self):
        assert TextWriteOpts(header="# Header\n").to_dict() == {"header": "# Header\n"}

    def test_with_header_footer_factory(self):
        opts = TextWriteOpts.with_(header="BEGIN\n", footer="\nEND")
        assert opts.header == "BEGIN\n"
        assert opts.footer == "\nEND"
        assert opts.delimiter == "\n"

    def test_repr(self):
        assert str(TextWriteOpts(delimiter="|")) == "TextWriteOpts(delimiter='|')"


class TestNormalizeOptions:
    def test_normalize_none(self):
        result = FileStream._normalize_options(None)
        assert result == {}

    def test_normalize_dict(self):
        input_dict = {"key": "value"}
        result = FileStream._normalize_options(input_dict)
        assert result == {"key": "value"}
        assert result is input_dict  # Should return same object

    def test_normalize_options_object(self):
        opts = FileOpts(encoding="utf-8")
        result = FileStream._normalize_options(opts)
        assert result == {"encoding": "utf-8"}
        assert isinstance(result, dict)

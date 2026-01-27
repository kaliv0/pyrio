class Mappable:
    def to_dict(self):
        """Returns dict with only non-None values"""
        return {attr: val for attr, val in vars(self).items() if val is not None}

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(f'{attr}={val!r}' for attr, val in vars(self).items() if val is not None)})"


class FileOpts(Mappable):
    """Options for file opening - applies to all file formats"""

    def __init__(self, encoding=None, errors=None, newline=None, buffering=None, mode=None):
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.buffering = buffering
        self.mode = mode

    @staticmethod
    def utf8(errors=None):
        """Creates FileOptions with UTF-8 encoding"""
        return FileOpts(encoding="utf-8", errors=errors)

    @staticmethod
    def ascii(errors=None):
        """Creates FileOptions with ASCII encoding"""
        return FileOpts(encoding="ascii", errors=errors)


# CSV
class CsvReadOpts(Mappable):
    """Options for reading CSV files"""

    def __init__(
        self,
        delimiter=None,
        quotechar=None,
        escapechar=None,
        doublequote=None,
        skipinitialspace=None,
        strict=None,
        dialect=None,
        fieldnames=None,
        restkey=None,
        restval=None,
    ):
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.skipinitialspace = skipinitialspace
        self.strict = strict
        self.dialect = dialect
        self.fieldnames = fieldnames
        self.restkey = restkey
        self.restval = restval

    @staticmethod
    def excel():
        """Creates CsvReadOptions for Excel CSV dialect"""
        return CsvReadOpts(dialect="excel")

    @staticmethod
    def unix():
        """Creates CsvReadOptions for Unix CSV dialect"""
        return CsvReadOpts(dialect="unix")


class CsvWriteOpts(Mappable):
    """Options for writing CSV files"""

    def __init__(
        self,
        delimiter=None,
        quotechar=None,
        escapechar=None,
        doublequote=None,
        skipinitialspace=None,
        strict=None,
        lineterminator=None,
        quoting=None,
        dialect=None,
        fieldnames=None,
        restval=None,
        extrasaction=None,
    ):
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.skipinitialspace = skipinitialspace
        self.strict = strict
        self.lineterminator = lineterminator
        self.quoting = quoting
        self.dialect = dialect
        self.fieldnames = fieldnames
        self.restval = restval
        self.extrasaction = extrasaction


class JsonReadOpts(Mappable):
    """Options for reading JSON files"""

    def __init__(
        self,
        parse_float=None,
        parse_int=None,
        object_hook=None,
        object_pairs_hook=None,
        cls=None,
    ):
        self.parse_float = parse_float
        self.parse_int = parse_int
        self.object_hook = object_hook
        self.object_pairs_hook = object_pairs_hook
        self.cls = cls

    @staticmethod
    def with_decimal():
        """Creates JsonReadOptions that parses floats as Decimal"""
        from decimal import Decimal

        return JsonReadOpts(parse_float=Decimal)


class JsonWriteOpts(Mappable):
    """Options for writing JSON files"""

    def __init__(
        self,
        indent=None,
        separators=None,
        sort_keys=None,
        default=None,
        ensure_ascii=None,
        allow_nan=None,
        skipkeys=None,
        cls=None,
    ):
        self.indent = indent
        self.separators = separators
        self.sort_keys = sort_keys
        self.default = default
        self.ensure_ascii = ensure_ascii
        self.allow_nan = allow_nan
        self.skipkeys = skipkeys
        self.cls = cls

    @staticmethod
    def pretty(indent=2):
        """Creates JsonWriteOptions for pretty-printed output"""
        return JsonWriteOpts(indent=indent)

    @staticmethod
    def compact():
        """Creates JsonWriteOptions for compact output with minimal whitespace"""
        return JsonWriteOpts(separators=(",", ":"))

    @staticmethod
    def sorted(indent=2):
        """Creates JsonWriteOptions with sorted keys and pretty-printing"""
        return JsonWriteOpts(indent=indent, sort_keys=True)


class YamlReadOpts(Mappable):
    """Options for reading YAML files"""

    def __init__(self, loader=None):  # noqa
        self.loader = loader


class YamlWriteOpts(Mappable):
    """Options for writing YAML files"""

    def __init__(
        self,
        default_flow_style=None,
        allow_unicode=None,
        indent=None,
        width=None,
    ):
        self.default_flow_style = default_flow_style
        self.allow_unicode = allow_unicode
        self.indent = indent
        self.width = width

    @staticmethod
    def block_style(indent=2):
        """Creates YamlWriteOptions for block-style output"""
        return YamlWriteOpts(default_flow_style=False, indent=indent)

    @staticmethod
    def flow_style():
        """Creates YamlWriteOptions for flow-style (inline) output"""
        return YamlWriteOpts(default_flow_style=True)


class XmlReadOpts(Mappable):
    """Options for reading XML files"""

    def __init__(
        self,
        process_namespaces=None,
        namespaces=None,
        attr_prefix=None,
        cdata_key=None,
    ):
        self.process_namespaces = process_namespaces
        self.namespaces = namespaces
        self.attr_prefix = attr_prefix
        self.cdata_key = cdata_key


class XmlWriteOpts(Mappable):
    """Options for writing XML files"""

    def __init__(
        self,
        pretty=None,
        indent=None,
        short_empty_elements=None,
    ):
        self.pretty = pretty
        self.indent = indent
        self.short_empty_elements = short_empty_elements

    @staticmethod
    def pretty(indent=4):
        """Creates XmlWriteOptions for pretty-printed output"""
        return XmlWriteOpts(pretty=True, indent=indent)


class TextWriteOpts(Mappable):
    """Options for writing plain text files"""

    def __init__(
        self,
        delimiter=None,
        header=None,
        footer=None,
    ):
        self.delimiter = delimiter
        self.header = header
        self.footer = footer

    @staticmethod
    def with_(*, header="", footer="", delimiter="\n"):
        """Creates PlainTextWriteOptions with header and footer"""
        return TextWriteOpts(delimiter=delimiter, header=header, footer=footer)

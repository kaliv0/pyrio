import importlib
import shutil
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path

from aldict import AliasDict

from pyrio.decorators import handle_consumed, pre_call, terminal
from pyrio.exceptions import NoneTypeError, UnknownSuffixError, UnsupportedFormatError
from pyrio.streams import BaseStream, Stream
from pyrio.utils import DictItem

TEMP_PATH = "{file_path}.tmp"


DSV_CONFIG = {
    ".csv": {
        "delimiter": ",",
    },
    ".tsv": {
        "delimiter": "\t",
    },
}

MAPPING_READ_CONFIG = AliasDict(
    {
        ".toml": {
            "import_mod": "tomllib",
            "callable": "load",
            "read_mode": "rb",
            "error": "TOMLDecodeError",
        },
        ".json": {
            "import_mod": "json",
            "callable": "load",
            "read_mode": "r",
            "error": "JSONDecodeError",
            "wrap_scalars": True,
        },
        ".yaml": {
            "import_mod": "pyrio.io.yaml_handler",
            "callable": "load",
            "read_mode": "r",
            "error": "YAMLError",
            "wrap_scalars": True,
        },
        ".xml": {
            "import_mod": "pyrio.io.xml_handler",
            "callable": "load",
            "read_mode": "rb",
            "error": "ExpatError",
            "extra_keys": ("include_root",),
        },
        ".ini": {
            "import_mod": "pyrio.io.ini_handler",
            "callable": "load",
            "read_mode": "r",
            "error": "ConfigParserError",
        },
        ".pickle": {
            "import_mod": "pickle",
            "callable": "load",
            "read_mode": "rb",
            "error": "UnpicklingError",
            "wrap_scalars": True,
        },
    },
    aliases={".yaml": ".yml", ".ini": ".cfg", ".pickle": ".pkl"},
)

MAPPING_WRITE_CONFIG = AliasDict(
    {
        ".toml": {
            "import_mod": "tomli_w",
            "callable": "dump",
            "write_mode": "wb",
            "default_null_handler": DictItem.replace_null("N/A"),
        },
        ".json": {
            "import_mod": "json",
            "callable": "dump",
            "write_mode": "w",
        },
        ".yaml": {
            "import_mod": "yaml",
            "callable": "dump",
            "write_mode": "w",
        },
        ".xml": {
            "import_mod": "pyrio.io.xml_handler",
            "callable": "dump",
            "write_mode": "w",
            "extra_keys": ("xml_root",),
        },
        ".ini": {
            "import_mod": "pyrio.io.ini_handler",
            "callable": "dump",
            "write_mode": "w",
        },
        ".pickle": {
            "import_mod": "pickle",
            "callable": "dump",
            "write_mode": "wb",
        },
    },
    aliases={".yaml": ".yml", ".ini": ".cfg", ".pickle": ".pkl"},
)

PLAIN_SUFFIXES = {
    ".txt",
    ".log",
    ".md",
    ".text",
    ".rst",
    ".out",
    ".err",
    ".diff",
    ".patch",
    ".adoc",
    ".wiki",
}

SNIFF_FORMATS = (
    # NB: we skip sniffing for:
    # - pickle (wrong bytes can do "bad things")
    # - csv/tsv (too permissive, can easily steal plain text)
    # - yaml (PyYAML accepts many non-YAML strings as scalars,
    #       JSON can also load as YAML since it's largely a subset)
    ".json",
    ".toml",
    ".xml",
)


@pre_call(handle_consumed)
class FileStream(BaseStream):
    """Derived Stream class for querying files; maps file content to im-memory dict structures and vice versa"""

    # Dirty deeds for a nice-looking API
    def __init__(self, file_path):  # noqa
        """Creates Stream from a file"""
        pass

    def __new__(cls, file_path, f_open=None, f_read=None, format=None, default_to_plain=False, **kwargs):
        obj = super().__new__(cls)
        if file_path is None:
            raise NoneTypeError("File path cannot be None")

        iterable = cls._try_read(file_path, f_open, f_read, format, default_to_plain, **kwargs)
        super(cls, obj).__init__(iterable)
        obj._file_path = file_path
        return obj

    @classmethod
    def process(cls, file_path, *, f_open=None, f_read=None, format=None, default_to_plain=False, **kwargs):
        """Creates Stream from a file with advanced reading options.

        'format' forces a file reader (bare or dotted, e.g. 'json' / '.json'),
        on failure raises (unless default_to_plain=True).

        'default_to_plain' skips format 'sniffing' when 'format' param is unset,
        allows plain fallback when 'format' is set
        """
        return cls.__new__(cls, file_path, f_open, f_read, format, default_to_plain, **kwargs)

    # ### reading from file ###
    @classmethod
    def _try_read(cls, file_path, f_open=None, f_read=None, format=None, default_to_plain=False, **kwargs):
        path = cls._get_file_path(file_path)
        f_open = f_open or {}
        f_read = f_read or {}

        forced_format = format is not None
        suffix = cls._normalize_format(format) if forced_format else path.suffix
        formats = [suffix]
        if not (forced_format or default_to_plain):
            # keep SNIFF_FORMATS order
            formats += [fmt for fmt in SNIFF_FORMATS if fmt != suffix]

        for i, fmt in enumerate(formats):
            # NB: caller f_read is format-specific - subsequent sniff candidates get empty config
            data, err = cls._read_file(path, fmt, f_open, f_read if i == 0 else {}, **kwargs)
            if err is None:
                return data
            if isinstance(err, UnicodeDecodeError):
                raise err
            if forced_format and not default_to_plain:
                raise err

        data, err = cls._read_plain(path, f_open)
        if err is not None:
            raise err
        return data

    @staticmethod
    def _normalize_format(fmt):
        import itertools

        if not (isinstance(fmt, str) and (name := fmt.strip().lower())):
            raise UnsupportedFormatError(f"Invalid format: {fmt!r}")

        if not name.startswith("."):
            name = f".{name}"

        if name not in itertools.chain(DSV_CONFIG, MAPPING_READ_CONFIG, PLAIN_SUFFIXES):
            raise UnsupportedFormatError(f"Unsupported format: {fmt!r}")
        return name

    @classmethod
    def _read_file(cls, path, suffix, f_open=None, f_read=None, **kwargs):
        if suffix in DSV_CONFIG:
            return cls._read_dsv(path, suffix, f_open, f_read)
        elif suffix in MAPPING_READ_CONFIG:
            return cls._read_mapping(path, suffix, f_open, f_read, **kwargs)
        elif suffix in PLAIN_SUFFIXES:
            return cls._read_plain(path, f_open)
        return None, UnknownSuffixError()

    @classmethod
    def _read_dsv(cls, path, suffix, f_open, f_read):
        import csv

        f_open = {"newline": "", **f_open}
        f_read = {"delimiter": DSV_CONFIG[suffix]["delimiter"], **f_read}
        try:
            return cls._load_data(path, f_open, lambda f: tuple(csv.DictReader(f, **f_read))), None
        except (csv.Error, UnicodeDecodeError) as err:
            return None, err

    @classmethod
    def _read_mapping(cls, path, suffix, f_open, f_read, **kwargs):
        config = MAPPING_READ_CONFIG[suffix]
        mod = config["import_mod"]
        load = getattr(importlib.import_module(mod), config["callable"])
        parse_err = getattr(importlib.import_module(mod), config["error"])

        f_open = {"mode": config["read_mode"], **f_open}
        extra = {k: kwargs[k] for k in config.get("extra_keys", ()) if k in kwargs}

        def _mapping_loader(f):
            data = load(f, **f_read, **extra)
            if config.get("wrap_scalars") and not isinstance(data, (Mapping, list, tuple)):
                # make plain values iterable
                return (data,)
            return data

        try:
            return cls._load_data(path, f_open, _mapping_loader), None
        except (parse_err, UnicodeDecodeError) as err:
            return None, err

    @classmethod
    def _read_plain(cls, path, f_open):
        try:
            return cls._load_data(path, f_open, tuple), None
        except UnicodeDecodeError as err:
            return None, err

    @staticmethod
    def _load_data(path, f_open, loader):
        with open(path, **f_open) as f:
            return loader(f)

    # ### writing to file ###
    @terminal
    def save(
        self,
        file_path=None,
        *,
        f_open=None,
        f_write=None,
        null_handler=None,
        materialize="dict",
        **kwargs,
    ):
        """Writes Stream to a new file (or updates an existing one) with advanced 'writing' options passed by the user.

        'materialize' controls how the stream becomes the object passed to mapping dumpers (json/yaml/toml/xml/ini/pickle).
        One of: 'dict' (default), 'list', 'tuple', 'raw' (exactly one element),
        or a callable receiving this stream and returning the payload.
        """
        path, tmp_path = self._prepare_file_paths(file_path)

        f_open = f_open or {}
        f_write = f_write or {}

        if (suffix := path.suffix) in DSV_CONFIG:
            self._write_dsv(path, tmp_path, f_open, f_write, null_handler)
        elif suffix in MAPPING_WRITE_CONFIG:
            self._write_mapping(
                path, tmp_path, f_open, f_write, null_handler, materialize=materialize, **kwargs
            )
        else:
            self._write_plain(path, tmp_path, f_open, f_write)

    def _write_dsv(self, path, tmp_path, f_open, f_write, null_handler=None):
        import csv

        if null_handler:
            self.map(null_handler)
        output = self.map(lambda x: Stream(x).to_dict()).to_tuple()

        f_open = {"mode": "w", **f_open}
        f_write = {
            "delimiter": DSV_CONFIG[path.suffix]["delimiter"],
            "fieldnames": output[0].keys() if output else (),
            **f_write,
        }
        with self._atomic_write(path, tmp_path, f_open) as f:  # noqa
            writer = csv.DictWriter(f, **f_write)
            writer.writeheader()
            writer.writerows(output)

    def _write_mapping(
        self, path, tmp_path, f_open, f_write, null_handler=None, materialize="dict", **kwargs
    ):
        config = MAPPING_WRITE_CONFIG[path.suffix]
        if existing_null_handler := null_handler or config.get("default_null_handler"):
            self.map(existing_null_handler)  # noqa

        output = self._materialize(materialize)
        extra = {k: kwargs[k] for k in config.get("extra_keys", ()) if k in kwargs}
        f_open = {"mode": config["write_mode"], **f_open}

        dump = getattr(importlib.import_module(config["import_mod"]), config["callable"])
        with self._atomic_write(path, tmp_path, f_open) as f:  # noqa
            dump(output, f, **f_write, **extra)

    def _materialize(self, materialize):
        if callable(materialize):
            return materialize(self)

        if materialize in {"dict", "list", "tuple"}:
            return getattr(self, f"to_{materialize}")()

        if materialize == "raw":
            if len(items := self.to_list()) != 1:
                raise ValueError(f"materialize='raw' requires exactly 1 element, got {len(items)}")
            return items[0]

        raise ValueError(
            f"Unsupported materialize={materialize!r}; expected 'dict', 'list', 'tuple', 'raw', or callable"
        )

    def _write_plain(self, path, tmp_path, f_open, f_write):
        open_opts = {"mode": "w", **f_open}

        output = self.to_string(f_write.get("delimiter", "\n"))
        header = f_write.get("header", "")
        footer = f_write.get("footer", "")
        if header or footer:
            output = f"{header}{output}{footer}"

        with self._atomic_write(path, tmp_path, open_opts) as f:  # noqa
            f.write(output)

    # ### helpers ###
    @staticmethod
    def _get_file_path(file_path, read_mode=True):
        path = Path(file_path)
        if read_mode and not path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")
        if path.is_dir():
            raise IsADirectoryError(f"Given path '{file_path}' is a directory")
        return path

    def _prepare_file_paths(self, file_path):
        if file_path is None:
            file_path = self._file_path
        path = self._get_file_path(file_path, read_mode=False)
        tmp_path = Path(TEMP_PATH.format(file_path=path))
        if tmp_path.exists():
            # So sorry Montessori...
            tmp_path.unlink(missing_ok=True)
        return path, tmp_path

    @contextmanager
    def _atomic_write(self, path, tmp_path, f_open):
        try:
            if f_open["mode"] == "a" and path.exists():
                tmp_path = shutil.copyfile(path, tmp_path)

            with open(tmp_path, **f_open) as f:
                yield f
            shutil.move(tmp_path, path)
        except (IOError, Exception) as e:
            tmp_path.unlink(missing_ok=True)
            raise e

    def __repr__(self):
        try:
            n = len(self.iterable)
        except TypeError:
            count = ""
        else:
            count = f", {n} elements"

        return f"{self.__class__.__name__}.of({self._file_path!r}{count})"

import importlib
import shutil
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path

from aldict import AliasDict

from pyrio.decorators import handle_consumed, pre_call, terminal
from pyrio.exceptions import NoneTypeError
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
        },
        ".json": {
            "import_mod": "json",
            "callable": "load",
            "read_mode": "r",
            "wrap_scalars": True,
        },
        ".yaml": {
            "import_mod": "pyrio.io.yaml_handler",
            "callable": "load",
            "read_mode": "r",
            "wrap_scalars": True,
        },
        ".xml": {
            "import_mod": "pyrio.io.xml_handler",
            "callable": "load",
            "read_mode": "rb",
            "extra_keys": ("include_root",),
        },
        ".ini": {
            "import_mod": "pyrio.io.ini_handler",
            "callable": "load",
            "read_mode": "r",
        },
        ".pickle": {
            "import_mod": "pickle",
            "callable": "load",
            "read_mode": "rb",
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
            "default_null_handler": lambda x: DictItem(x.key, "N/A") if x.value is None else x,
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


@pre_call(handle_consumed)
class FileStream(BaseStream):
    """Derived Stream class for querying files; maps file content to im-memory dict structures and vice versa"""

    # Dirty deeds for a nice-looking API
    def __init__(self, file_path):  # noqa
        """Creates Stream from a file"""
        pass

    def __new__(cls, file_path, f_open=None, f_read=None, **kwargs):
        obj = super().__new__(cls)
        if file_path is None:
            raise NoneTypeError("File path cannot be None")

        iterable = cls._read_file(file_path, f_open, f_read, **kwargs)
        super(cls, obj).__init__(iterable)
        obj._file_path = file_path
        return obj

    @classmethod
    def process(cls, file_path, *, f_open=None, f_read=None, **kwargs):
        """Creates Stream from a file with advanced 'reading' options passed by the user"""
        return cls.__new__(cls, file_path, f_open, f_read, **kwargs)

    # ### reading from file ###
    @classmethod
    def _read_file(cls, file_path, f_open=None, f_read=None, **kwargs):
        path = cls._get_file_path(file_path)

        f_open = f_open or {}
        f_read = f_read or {}

        if (suffix := path.suffix) in DSV_CONFIG:
            return cls._read_dsv(path, f_open, f_read)
        elif suffix in MAPPING_READ_CONFIG:
            return cls._read_mapping(path, f_open, f_read, **kwargs)
        else:
            return cls._read_plain(path, f_open)

    @classmethod
    def _read_dsv(cls, path, f_open, f_read):
        import csv

        cls._prepare_io_options(
            [
                (f_open, "newline", ""),
                (f_read, "delimiter", DSV_CONFIG[path.suffix]["delimiter"]),
            ]
        )
        return cls._load_data(path, f_open, lambda f: tuple(csv.DictReader(f, **f_read)))

    @classmethod
    def _read_mapping(cls, path, f_open, f_read, **kwargs):
        config = MAPPING_READ_CONFIG[path.suffix]
        load = getattr(importlib.import_module(config["import_mod"]), config["callable"])

        cls._prepare_io_options([(f_open, "mode", config["read_mode"])])
        extra = {k: kwargs[k] for k in config.get("extra_keys", ()) if k in kwargs}

        def _mapping_loader(f):
            data = load(f, **f_read, **extra)
            if config.get("wrap_scalars") and not isinstance(data, (Mapping, list, tuple)):
                # make plain values iterable
                return (data,)
            return data

        return cls._load_data(path, f_open, _mapping_loader)

    @classmethod
    def _read_plain(cls, path, f_open):
        return cls._load_data(path, f_open, tuple)

    @staticmethod
    def _load_data(path, f_open, loader):
        with open(path, **f_open) as f:
            data = loader(f)
        return data

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

        self._prepare_io_options(
            [
                (f_open, "mode", "w"),
                (f_write, "delimiter", DSV_CONFIG[path.suffix]["delimiter"]),
                (f_write, "fieldnames", output[0].keys() if output else ()),
            ]
        )
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
        self._prepare_io_options([(f_open, "mode", config["write_mode"])])

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
        self._prepare_io_options([(f_open, "mode", "w")])

        output = self.to_string(f_write.pop("delimiter", "\n"))
        header = f_write.pop("header", "")
        footer = f_write.pop("footer", "")
        if header or footer:
            output = f"{header}{output}{footer}"

        with self._atomic_write(path, tmp_path, f_open) as f:  # noqa
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

    @staticmethod
    def _prepare_io_options(settings):
        for options, key, value in settings:
            options.setdefault(key, value)

    @contextmanager
    def _atomic_write(self, path, tmp_path, f_open):
        try:
            if f_open["mode"] == "a":
                tmp_path = shutil.copyfile(path, tmp_path)

            with open(tmp_path, **f_open) as f:
                yield f
            shutil.move(tmp_path, path)
        except (IOError, Exception) as e:
            tmp_path.unlink(missing_ok=True)
            raise e

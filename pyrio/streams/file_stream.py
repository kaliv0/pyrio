import importlib
from contextlib import contextmanager
from pathlib import Path

from aldict import AliasDict

from pyrio.utils import DictItem
from pyrio.streams import BaseStream, Stream
from pyrio.exceptions import NoneTypeError, FileProcessingError

TEMP_PATH = "{file_path}.bak"
DSV_TYPES = {".csv", ".tsv"}
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
        },
        ".yaml": {
            "import_mod": "yaml",
            "callable": "safe_load",
            "read_mode": "r",
        },
        ".xml": {
            "import_mod": "xmltodict",
            "callable": "parse",
            "read_mode": "rb",
        },
    }
)
MAPPING_READ_CONFIG.add_alias(".yaml", ".yml")

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
            "default_null_handler": None,
        },
        ".yaml": {
            "import_mod": "yaml",
            "callable": "dump",
            "write_mode": "w",
            "default_null_handler": None,
        },
        ".xml": {
            "import_mod": "xmltodict",
            "callable": "unparse",
            "write_mode": "w",
            "default_null_handler": None,
        },
    }
)
MAPPING_WRITE_CONFIG.add_alias(".yaml", ".yml")


class FileStream(BaseStream):
    """Derived Stream class for querying files; maps file content to im-memory dict structures and vice versa"""

    # NB: Dirty deeds for a nice-looking API
    def __init__(self, file_path):  # noqa
        """Creates Stream from a file"""
        pass

    def __new__(cls, file_path, f_open_options=None, f_read_options=None, **kwargs):
        obj = super().__new__(cls)
        if file_path is None:
            raise NoneTypeError("File path cannot be None")
        file_handler, iterable = cls._read_file(file_path, f_open_options, f_read_options, **kwargs)
        super(cls, obj).__init__(iterable)
        obj._file_path = file_path
        obj._file_handler = file_handler
        obj._on_close_handler = lambda: obj._file_handler.close() if not obj._file_handler.closed else None
        return obj

    @classmethod
    def process(cls, file_path, *, f_open_options=None, f_read_options=None, **kwargs):
        """Creates Stream from a file with advanced 'reading' options passed by the user"""
        return cls.__new__(cls, file_path, f_open_options, f_read_options, **kwargs)

    # ### reading from file ###
    @classmethod
    def _read_file(cls, file_path, f_open_options=None, f_read_options=None, **kwargs):
        path = cls._get_file_path(file_path)

        if f_open_options is None:
            f_open_options = {}
        if f_read_options is None:
            f_read_options = {}

        try:
            if (suffix := path.suffix) in DSV_TYPES:
                return cls._read_dsv(path, f_open_options, f_read_options)
            elif suffix in MAPPING_READ_CONFIG.keys():
                return cls._read_mapping(path, f_open_options, f_read_options, **kwargs)
            else:
                return cls._read_plain(path, f_open_options)
        except (IOError, Exception) as e:
            raise FileProcessingError(f"Something went wrong while processing '{file_path}'") from e

    @staticmethod
    def _read_dsv(path, f_open_options, f_read_options):
        import csv

        if "newline" not in f_open_options:
            f_open_options["newline"] = ""
        FileStream._prepare_io_options(
            [("delimiter", f_read_options, lambda: "\t" if path.suffix == ".tsv" else ",")]
        )

        file_handler = open(path, **f_open_options)
        return file_handler, tuple(csv.DictReader(file_handler, **f_read_options))

    @staticmethod
    def _read_mapping(path, f_open_options, f_read_options, **kwargs):
        config = MAPPING_READ_CONFIG[path.suffix]
        load = getattr(importlib.import_module(config["import_mod"]), config["callable"])
        FileStream._prepare_io_options([("mode", f_open_options, lambda: config["read_mode"])])

        file_handler = open(path, **f_open_options)
        content = load(file_handler, **f_read_options)
        if path.suffix == ".xml":
            if kwargs.get("include_root", None):
                return file_handler, content
            # NB: return dict (instead of dict_view) to re-map it later as DictItem records
            return file_handler, next(iter(content.values()))
        return file_handler, content

    @staticmethod
    def _read_plain(path, f_open_options):
        # NB: don't pass 'mode' explicitly and use the default one ('r')
        file_handler = open(path, **f_open_options)
        return file_handler, (line for line in file_handler)

    # ### writing to file ###
    def save(self, file_path=None, *, f_open_options=None, f_write_options=None, null_handler=None, **kwargs):
        """Writes Stream to a new file (or updates an existing one) with advanced 'writing' options passed by the user"""
        path, tmp_path = self._prepare_file_paths(file_path)

        if f_open_options is None:
            f_open_options = {}
        if f_write_options is None:
            f_write_options = {}

        if (suffix := path.suffix) in DSV_TYPES:
            return self._write_dsv(path, tmp_path, f_open_options, f_write_options, null_handler)
        elif suffix in MAPPING_READ_CONFIG.keys():
            return self._write_mapping(
                path, tmp_path, f_open_options, f_write_options, null_handler, **kwargs
            )
        else:
            return self._write_plain(path, tmp_path, f_open_options)

    def _write_dsv(self, path, tmp_path, f_open_options, f_write_options, null_handler=None):
        import csv

        if null_handler:
            self.map(null_handler)
        output = self.map(lambda x: Stream(x).to_dict()).to_tuple()

        self._prepare_io_options(
            [
                ("mode", f_open_options, lambda: "w"),
                ("delimiter", f_write_options, lambda: "\t" if path.suffix == ".tsv" else ","),
            ]
        )
        if "fieldnames" not in f_write_options:
            f_write_options["fieldnames"] = output[0].keys() if output else ()

        with self._atomic_write(path, tmp_path, f_open_options) as f:
            writer = csv.DictWriter(f, **f_write_options)
            writer.writeheader()
            writer.writerows(output)

    def _write_mapping(self, path, tmp_path, f_open_options, f_write_options, null_handler=None, **kwargs):
        config = MAPPING_WRITE_CONFIG[path.suffix]
        if existing_null_handler := null_handler or config["default_null_handler"]:
            self.map(existing_null_handler)

        output = self.to_dict()

        self._prepare_io_options([("mode", f_open_options, lambda: config["write_mode"])])
        if path.suffix == ".xml":
            root = kwargs.get("xml_root", "root")
            output = {root: output}
            if "pretty" not in f_write_options:
                f_write_options["pretty"] = True

        dump = getattr(importlib.import_module(config["import_mod"]), config["callable"])
        with self._atomic_write(path, tmp_path, f_open_options) as f:
            dump(output, f, **f_write_options)

    # TODO: pass delimiter (default '\n' in w_write_opts)?
    def _write_plain(self, path, tmp_path, f_open_options=None):
        self._prepare_io_options([("mode", f_open_options, lambda: "w")])
        with self._atomic_write(path, tmp_path, f_open_options) as f:
            f.writelines(self.to_tuple())  # TODO: user should put \n with map() explicitly before save
            # for line in self:
            #     f.write(str(line))  # TODO: add \n here or custom delimiter from w_write_opts
            #                         #     if using loop -> explicitly invoke self._file_handler.close()

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
        tmp_path = Path(TEMP_PATH.format(file_path=self._file_path))
        if tmp_path.exists():
            # So sorry Montessori...
            tmp_path.unlink(missing_ok=True)
        return path, tmp_path

    @staticmethod
    def _prepare_io_options(settings):
        for key, f_options, supplier in settings:
            if key not in f_options:
                f_options[key] = supplier()

    # @staticmethod
    # def _prepare_delimiter(f_options, suffix):
    #     if "delimiter" not in f_options:
    #         f_options["delimiter"] = "\t" if suffix == ".tsv" else ","

    @contextmanager
    def _atomic_write(self, path, tmp_path, f_open_options):
        try:
            # NB: 'mode' is passed as part of f_open_options
            with open(tmp_path, **f_open_options) as f:
                yield f
            tmp_path.replace(path)
        except (IOError, Exception) as e:
            tmp_path.unlink(missing_ok=True)
            raise FileProcessingError(f"Something went wrong while saving to '{path}'") from e

import importlib
from pathlib import Path

from pyrio.utils.dict_item import Item
from pyrio.streams.base_stream import BaseStream
from pyrio.utils.exception import UnsupportedFileTypeError


READ_CONFIG = {
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

WRITE_CONFIG = {
    ".toml": {
        "import_mod": "tomli_w",
        "callable": "dump",
        "write_mode": "wb",
        "default_null_handler": lambda x: Item(x.key, "N/A") if x.value is None else x,
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


class FileStream(BaseStream):
    # NB: Dirty deeds for a nice-looking API
    def __init__(self, file_path):  # noqa
        """Creates Stream from a file"""
        pass

    def __new__(cls, file_path, f_open_options=None, f_read_options=None, **kwargs):
        obj = super().__new__(cls)
        iterable = cls._read_file(file_path, f_open_options, f_read_options, **kwargs)
        super(cls, obj).__init__(iterable)
        obj.file_path = file_path
        return obj

    @classmethod
    def process(cls, file_path, f_open_options=None, f_read_options=None, **kwargs):
        return cls.__new__(cls, file_path, f_open_options, f_read_options, **kwargs)

    # ### reading from file ###
    @classmethod
    def _read_file(cls, file_path, f_open_options=None, f_read_options=None, **kwargs):
        path = cls._get_file_path(file_path)

        if path.suffix in {".csv", ".tsv"}:
            return cls._read_csv(path, f_open_options, f_read_options, **kwargs)
        return cls._read_binary(path, f_open_options, f_read_options, **kwargs)

    @staticmethod
    def _read_csv(path, f_open_options=None, f_read_options=None, **kwargs):
        import csv

        if f_read_options is None:
            f_read_options = {}
        f_read_options["delimiter"] = "\t" if path.suffix == ".tsv" else ","

        with open(path, **(f_open_options or {})) as f:
            return tuple(csv.DictReader(f, **f_read_options))

    @staticmethod
    def _read_binary(path, f_open_options=None, f_read_options=None, **kwargs):
        suffix = ".yaml" if path.suffix == ".yml" else path.suffix
        if suffix not in READ_CONFIG:
            raise UnsupportedFileTypeError(f"Unsupported file type: '{suffix}'")

        config = READ_CONFIG[suffix]
        load = getattr(importlib.import_module(config["import_mod"]), config["callable"])

        with open(path, config["read_mode"], **(f_open_options or {})) as f:
            content = load(f, **(f_read_options or {}))
            if suffix == ".xml":
                if kwargs.get("include_root", None):
                    return content
                # NB: return dict (instead of dict_view) to re-map it later as Item records
                return next(iter(content.values()))
            return content

    #####################################################################################################################################################
    # ### writing to file ###
    def save(
        self, file_path=None, null_handler=None, f_open_options=None, f_write_options=None, **kwargs
    ):
        if file_path is None:
            file_path = self.file_path
        path = self._get_file_path(file_path, read_mode=False)

        # if path.suffix in {".csv", ".tsv"}:
        #     return self._save_csv(path, **kwargs)
        return self._write_file(path, null_handler, f_open_options, f_write_options, **kwargs)

    def _write_file(
        self, path, null_handler=None, f_open_options=None, f_write_options=None, **kwargs
    ):
        suffix = ".yaml" if path.suffix == ".yml" else path.suffix
        if suffix not in WRITE_CONFIG:
            raise UnsupportedFileTypeError(f"Unsupported file type: '{suffix}'")

        config = WRITE_CONFIG[suffix]
        dump = getattr(importlib.import_module(config["import_mod"]), config["callable"])

        if existing_null_handler := null_handler or config["default_null_handler"]:
            self.map(existing_null_handler)

        output = self.to_dict(lambda x: (x.key, x.value))
        root = kwargs.get("xml_root", "root")
        if suffix == ".xml":
            output = {root: output}
            f_write_options["pretty"] = True

        with open(path, config["write_mode"], **(f_open_options or {})) as f:
            dump(output, f, **(f_write_options or {}))

    # ### helpers ###
    @staticmethod
    def _get_file_path(file_path, read_mode=True):
        path = Path(file_path)
        if read_mode and path.exists() is False:
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")
        if path.is_dir():
            raise IsADirectoryError(f"Given path '{file_path}' is a directory")
        return path

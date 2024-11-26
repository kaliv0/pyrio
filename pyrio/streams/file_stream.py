import importlib
from pathlib import Path

from pyrio.utils.dict_item import Item
from pyrio.streams.base_stream import BaseStream
from pyrio.utils.exception import UnsupportedFileTypeError

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
    ".yml": {
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

    def __new__(cls, file_path, **kwargs):
        obj = super().__new__(cls)
        iterable = cls._read_file(file_path, **kwargs)
        super(cls, obj).__init__(iterable)
        obj.file_path = file_path
        return obj

    @classmethod
    def process(cls, file_path, **kwargs):
        return cls.__new__(cls, file_path, **kwargs)

    # ### reading from file ###
    @classmethod
    def _read_file(cls, file_path, **kwargs):
        path = cls._read_file_path(file_path)

        if path.suffix in {".csv", ".tsv"}:
            return cls._read_csv(path, **kwargs)
        return cls._read_binary(path, **kwargs)

    @staticmethod
    def _read_file_path(file_path):
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")
        if path.is_dir():
            raise IsADirectoryError(f"Given path '{file_path}' is a directory")
        return path

    @staticmethod
    def _read_csv(path, **kwargs):
        import csv

        with open(path) as f:
            delimiter = "\t" if path.suffix == ".tsv" else ","
            return tuple(csv.DictReader(f, delimiter=delimiter, **kwargs))

    @staticmethod
    def _read_binary(path, **kwargs):
        # TODO: refactor with READ_CONFIG?
        with open(path, "rb") as f:
            match path.suffix:
                case ".toml":
                    import tomllib

                    return tomllib.load(f, **kwargs)
                case ".json":
                    import json

                    return json.load(f, **kwargs)
                case ".yaml" | ".yml":
                    import yaml

                    return yaml.safe_load(f)
                case ".xml":
                    import xmltodict

                    return xmltodict.parse(f, **kwargs).get("root")
                    # TODO: what if in rare cases it's not root -> user should point? -> or simply keep the root
                case _:
                    raise UnsupportedFileTypeError(f"Unsupported file type: '{path.suffix}'")

    #####################################################################################################################################################
    # ### writing to file ###
    def save(self, file_path=None, null_handler=None, f_open_options=None, f_save_options=None):
        if file_path is None:
            file_path = self.file_path
        # TODO: refactor -> of we re-using file parsing to Path object is already done and check for is_dir() is redundant
        path = Path(file_path)
        if path.is_dir():
            raise IsADirectoryError(f"Given path '{file_path}' is a directory")

        # if path.suffix in {".csv", ".tsv"}:
        #     return self._save_csv(path, **kwargs)
        return self._write_file(path, null_handler, f_open_options, f_save_options)

    def _write_file(self, path, null_handler=None, f_open_options=None, f_save_options=None):
        if path.suffix not in WRITE_CONFIG:
            raise UnsupportedFileTypeError(f"Unsupported file type: '{path.suffix}'")

        config = WRITE_CONFIG[path.suffix]
        dump = getattr(importlib.import_module(config["import_mod"]), config["callable"])

        if existing_null_handler := null_handler or config["default_null_handler"]:
            self.map(existing_null_handler)

        output = self.to_dict(lambda x: (x.key, x.value))
        if path.suffix == ".xml":
            # TODO: give user access to 'root' param -> use dicttoxml library for more options??
            output = {"root": output}
            f_save_options["pretty"] = True

        with open(path, config["write_mode"], **(f_open_options or {})) as f:
            dump(output, f, **(f_save_options or {}))

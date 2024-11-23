from pathlib import Path

from pyrio.streams.base_stream import BaseStream
from pyrio.utils.exception import UnsupportedFileTypeError


class FileStream(BaseStream):
    # NB: how naughty can it be?!
    def __init__(self, file_path):  # noqa
        """Creates Stream from a file"""
        ...

    def __new__(cls, file_path, **kwargs):
        obj = super().__new__(cls)
        iterable = cls._read_file(file_path, **kwargs)
        super(cls, obj).__init__(iterable)
        return obj

    @classmethod
    def process(cls, file_path, **kwargs):
        return cls.__new__(cls, file_path, **kwargs)

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

        with open(path, newline="") as f:
            delimiter = "\t" if path.suffix == ".tsv" else ","
            return tuple(csv.DictReader(f, delimiter=delimiter, **kwargs))

    @staticmethod
    def _read_binary(path, **kwargs):
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
                case _:
                    raise UnsupportedFileTypeError(f"Unsupported file type: '{path.suffix}'")

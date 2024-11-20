from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyrio.base_stream import BaseStream
from pyrio.exception import UnsupportedFileTypeError


@dataclass(frozen=True, slots=True)
class Item:
    key: Any
    value: Any


class FileStream(BaseStream):
    # used internally
    # TODO: do we need this explicitly?
    # def __init__(self, iterable):
    #     super().__init__(iterable)

    @classmethod
    def of(cls, file_path):
        """Creates Stream from a file"""
        content_dict = cls._read_file(file_path)
        iterable = (Item(key=k, value=v) for k, v in content_dict.items())
        return cls(iterable)

    # TODO: provide access for csv.DictReader options via **kwargs -> add tests
    @classmethod
    def of_csv(cls, file_path):
        """Creates Stream from a csv file"""
        iterable = cls._read_csv_file(file_path)
        return cls(iterable)

    @classmethod
    def _read_file(cls, file_path):
        path = cls.__read_file_path(file_path)
        with open(path, "rb") as f:
            match path.suffix:
                case ".toml":
                    import tomllib

                    return tomllib.load(f)
                case ".json":
                    import json

                    return json.load(f)
                case ".yaml" | ".yml":
                    import yaml

                    return yaml.safe_load(f)
                case ".xml":
                    import xmltodict

                    return xmltodict.parse(f).get("root")
                case _:
                    # TODO: add check and prompt to other method if .csv file
                    raise UnsupportedFileTypeError(f"Unsupported file type: '{path.suffix}'")

    @classmethod
    def _read_csv_file(cls, file_path):
        path = cls.__read_file_path(file_path)
        if path.suffix not in {".csv", ".tsv"}:
            # TODO: add check and prompt to other method if some of the other types
            raise UnsupportedFileTypeError(f"Unsupported file type: '{path.suffix}'")

        import csv

        with open(path, newline="") as f:
            delimiter = "\t" if path.suffix == ".tsv" else ","
            return tuple(csv.DictReader(f, delimiter=delimiter))

    @staticmethod
    def __read_file_path(file_path):
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")
        if path.is_dir():
            raise IsADirectoryError(f"Given path '{file_path}' is a directory")
        return path

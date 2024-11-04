from collections.abc import Mapping
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
    def __init__(self, file_path):
        """Creates Stream from a file"""
        iterable = self._read_file(file_path)
        if isinstance(iterable, Mapping):
            iterable = (Item(key=k, value=v) for k, v in iterable.items())
        super().__init__(iterable)

    @staticmethod
    def _read_file(file_path):
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")
        if path.is_dir():
            raise IsADirectoryError(f"Given path '{file_path}' is a directory")

        extension = path.suffix
        if extension in (".csv", ".tsv"):
            import csv

            with open(file_path, newline="") as f:
                delimiter = "\t" if extension == ".tsv" else ","
                return tuple(csv.DictReader(f, delimiter=delimiter))

        with open(file_path, "rb") as f:
            match extension:
                case ".toml":
                    import tomllib

                    return tomllib.load(f)
                case ".json":
                    import json

                    return json.load(f)
                case ".yaml":
                    import yaml

                    return yaml.safe_load(f)
                case ".xml":
                    import xmltodict

                    return xmltodict.parse(f).get("root")

                case _:
                    raise UnsupportedFileTypeError(f"Unsupported file type: '{extension}'")

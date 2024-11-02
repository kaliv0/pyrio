import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pyrio.base_stream import BaseStream


@dataclass(frozen=True)
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
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No such file or directory: {file_path}")
        # TODO: raise if path is dir

        _, extension = os.path.splitext(file_path)
        if extension == ".csv":  # TODO: handle .tsv files
            import csv

            with open(file_path, newline="") as f:
                return tuple(csv.DictReader(f))

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

                    return xmltodict.parse(f)["root"]

                case _:
                    raise TypeError("Unsupported file type")  # TODO

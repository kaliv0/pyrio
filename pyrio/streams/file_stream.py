from pathlib import Path

from pyrio.streams.base_stream import BaseStream
from pyrio.utils.exception import UnsupportedFileTypeError


class FileStream(BaseStream):
    # NB: Dirty deeds for nice-looking API's
    def __init__(self, file_path):  # noqa
        """Creates Stream from a file"""
        pass

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

                    # TODO: add checks for ill-formated file content?
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

    def save(self, file_path=None, **kwargs):
        # TODO: validate path -> if None, re-use same file
        path = Path(file_path)
        # if path.suffix in {".csv", ".tsv"}:
        #     return self._save_csv(path, **kwargs)
        return self._save_binary(path, **kwargs)

    def _save_binary(self, path, **kwargs):
        with open(path, "wb") as f:
            match path.suffix:
                case ".toml":
                    import tomli_w

                    output = self.to_dict(lambda x: (x.key, x.value))
                    tomli_w.dump(output, f, **kwargs)

                # case ".json":
                #     import json
                #
                #     return json.load(f, **kwargs)
                # case ".yaml" | ".yml":
                #     import yaml
                #
                #     return yaml.safe_load(f)
                # case ".xml":
                #     import xmltodict
                #
                #     return xmltodict.parse(f, **kwargs).get("root")
                case _:
                    raise UnsupportedFileTypeError(f"Unsupported file type: '{path.suffix}'")

# TODO: rename file?


class IllegalStateError(Exception):
    pass


class NullPointerError(Exception):
    pass


# TODO: maybe ValueError??
class NoSuchElementError(Exception):
    pass


class UnsupportedTypeError(TypeError):
    pass


class UnsupportedFileTypeError(UnsupportedTypeError):
    pass


class AliasError(ValueError):
    pass

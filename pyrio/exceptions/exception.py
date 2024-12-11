# TODO: rename file?


class IllegalStateError(Exception):
    pass


class NullPointerError(Exception):
    pass


class NoSuchElementError(Exception):
    pass


class UnsupportedTypeError(TypeError):
    pass


class UnsupportedFileTypeError(UnsupportedTypeError):
    pass

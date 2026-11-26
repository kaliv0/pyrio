try:
    from .file_stream import FileStream as FileStream
except ModuleNotFoundError as e:
    raise ModuleNotFoundError("pyrio.fs requires the 'fs' extra. Install with: pip install pyrio[fs]") from e

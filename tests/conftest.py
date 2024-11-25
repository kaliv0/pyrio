import pytest


@pytest.fixture
def Foo():
    class Foo:
        def __init__(self, name, num):
            self.name = name
            self.num = num

    return Foo


@pytest.fixture(scope="session")
def tmp_file_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("file_stream")

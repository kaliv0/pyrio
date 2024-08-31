import pytest


@pytest.fixture
def Foo():
    class Foo:
        def __init__(self, name, num):
            self.name = name
            self.num = num

    return Foo

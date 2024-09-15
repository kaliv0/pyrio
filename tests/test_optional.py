import pytest

from pyrio import Stream
from pyrio.exception import NoSuchElementError, NullPointerError
from pyrio.optional import Optional


def test_optional_get_raises():
    with pytest.raises(NoSuchElementError) as e:
        Stream.empty().find_first().get()
    assert str(e.value) == "Optional is empty"


def test_optional_of_none_raises():
    with pytest.raises(NullPointerError) as e:
        Optional.of(None)
    assert str(e.value) == "Value cannot be None"


def test_print_optional():
    assert str(Stream.of(1, 2, 3, 4).filter(lambda x: x % 2 == 0).find_first()) == "Optional[2]"
    assert str(Stream.empty().filter(lambda x: x % 2 == 0).find_first()) == "Optional[None]"

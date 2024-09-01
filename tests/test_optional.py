import pytest

from pyrio import Stream
from pyrio.optional import Optional
from pyrio.exception import NoSuchElementError, NullPointerError


def test_optional_get_raises():
    with pytest.raises(NoSuchElementError) as e:
        Stream.empty().find_first().get()
    assert str(e.value) == "Optional is empty"


def test_optional_of_none_raises():
    with pytest.raises(NullPointerError) as e:
        Optional.of(None)
    assert str(e.value) == "Value cannot be None"

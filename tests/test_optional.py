import io
from contextlib import redirect_stdout

import pytest

from pyrio.exception import NoSuchElementError, NullPointerError
from pyrio import Optional


def test_optional_get_raises():
    with pytest.raises(NoSuchElementError) as e:
        Optional.empty().get()
    assert str(e.value) == "Optional is empty"


def test_optional_of_none_raises():
    with pytest.raises(NullPointerError) as e:
        Optional.of(None)
    assert str(e.value) == "Value cannot be None"


def test_print_optional():
    assert str(Optional.of(2)) == "Optional[2]"
    assert str(Optional.of_nullable(None)) == "Optional[None]"


def test_is_empty():
    assert Optional.of(3).is_empty() is False
    assert Optional.of_nullable(None).is_empty()


def test_get():
    assert Optional.of(3).get() == 3


def test_is_present():
    assert Optional.of(3).is_present()


def test_if_present():
    f = io.StringIO()
    with redirect_stdout(f):
        Optional.of(3).if_present(action=lambda x: print(f"{x}", end=""))
    assert f.getvalue() == "3"


def test_if_present_or_else():
    f = io.StringIO()
    with redirect_stdout(f):
        Optional.of(3).if_present_or_else(
            action=lambda x: print(f"{x}", end=""), empty_action=lambda: print("BANG!", end="")
        )
    assert f.getvalue() == "3"


def test_if_present_or_else_empty_action():
    f = io.StringIO()
    with redirect_stdout(f):
        Optional.empty().if_present_or_else(
            action=lambda x: print(f"{x}", end=""), empty_action=lambda: print("BANG!", end="")
        )
    assert f.getvalue() == "BANG!"


def test_or_else():
    assert Optional.of(3).or_else(4) == 3
    assert Optional.empty().or_else(4) == 4


def test_or_else_get(Foo):
    foo = Foo(name="Foo", num=43)
    assert Optional.empty().or_else_get(supplier=lambda: foo) is foo

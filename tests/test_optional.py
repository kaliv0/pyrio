import io
from contextlib import redirect_stdout

import pytest

from pyrio import Optional
from pyrio.exceptions import NoSuchElementError, NoneTypeError


class TestOptional:
    def test_optional_get_raises(self):
        with pytest.raises(NoSuchElementError) as e:
            Optional.empty().get()
        assert str(e.value) == "Optional is empty"

    def test_optional_of_none_raises(self):
        with pytest.raises(NoneTypeError) as e:
            Optional.of(None)
        assert str(e.value) == "Value cannot be None"

    def test_print_optional(self):
        assert str(Optional.of(2)) == "Optional[2]"
        assert str(Optional.of_nullable(None)) == "Optional[None]"

    def test_is_empty(self):
        assert Optional.of(3).is_empty() is False
        assert Optional.of_nullable(None).is_empty()

    def test_get(self):
        assert Optional.of(3).get() == 3

    def test_is_present(self):
        assert Optional.of(3).is_present()

    def test_if_present(self):
        f = io.StringIO()
        with redirect_stdout(f):
            Optional.of(3).if_present(action=lambda x: print(f"{x}", end=""))
        assert f.getvalue() == "3"

    def test_if_present_or_else(self):
        f = io.StringIO()
        with redirect_stdout(f):
            Optional.of(3).if_present_or_else(
                action=lambda x: print(f"{x}", end=""), empty_action=lambda: print("BANG!", end="")
            )
        assert f.getvalue() == "3"

    def test_if_present_or_else_empty_action(self):
        f = io.StringIO()
        with redirect_stdout(f):
            Optional.empty().if_present_or_else(
                action=lambda x: print(f"{x}", end=""), empty_action=lambda: print("BANG!", end="")
            )
        assert f.getvalue() == "BANG!"

    def test_or_else(self):
        assert Optional.of(3).or_else(4) == 3
        assert Optional.empty().or_else(4) == 4

    def test_or_else_get(self, Foo):
        foo = Foo(name="Foo", num=43)
        assert Optional.empty().or_else_get(supplier=lambda: foo) is foo

    def test_or_else_raise(self, Foo):
        with pytest.raises(NoSuchElementError) as e:
            Optional.empty().or_else_raise()
        assert str(e.value) == "Optional is empty"

    def test_or_else_raise_custom_supplier(self, Foo):
        err_msg = "Yo Mr. White...!"

        class DamnItError(Exception):
            pass

        def damn_it_supplier():
            raise DamnItError(err_msg)

        with pytest.raises(DamnItError) as e:
            Optional.empty().or_else_raise(damn_it_supplier)
        assert str(e.value) == err_msg

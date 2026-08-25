import io
from contextlib import redirect_stdout

import pytest

from pyrio import Optional, Stream
from pyrio.exceptions import NoneTypeError, NoSuchElementError


def test_optional_get_raises():
    with pytest.raises(NoSuchElementError) as e:
        Optional.empty().get()
    assert str(e.value) == "Optional is empty"


def test_optional_of_none_raises():
    with pytest.raises(NoneTypeError) as e:
        Optional.of(None)
    assert str(e.value) == "Value cannot be None"


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


def test_or_else_optional():
    optional = Optional.of(3)
    assert optional.or_else_optional(supplier=lambda: Optional.of(99)) is optional


def test_or_else_optional_empty():
    assert Optional.empty().or_else_optional(supplier=lambda: Optional.of(99)).get() == 99
    assert Optional.empty().or_else_optional(supplier=Optional.empty).is_empty()


def test_or_else_raise(Foo):
    with pytest.raises(NoSuchElementError) as e:
        Optional.empty().or_else_raise()
    assert str(e.value) == "Optional is empty"


def test_or_else_raise_custom_supplier(Foo):
    err_msg = "Yo Mr. White...!"

    class DamnItError(Exception):
        pass

    def damn_it_supplier():
        raise DamnItError(err_msg)

    with pytest.raises(DamnItError) as e:
        Optional.empty().or_else_raise(damn_it_supplier)
    assert str(e.value) == err_msg


def test_map():
    assert Optional.of(3).map(lambda x: x * 2).get() == 6


def test_map_empty():
    assert Optional.empty().map(lambda x: x * 2).is_empty()


def test_map_returns_none():
    assert Optional.of(3).map(lambda x: None).is_empty()


def test_filter_matches():
    optional = Optional.of(3)
    assert optional.filter(lambda x: x > 0) is optional


def test_filter_no_match():
    assert Optional.of(3).filter(lambda x: x > 5).is_empty()


def test_filter_empty():
    assert Optional.empty().filter(lambda x: x > 0).is_empty()


def test_flat_map():
    assert Optional.of(3).flat_map(lambda x: Optional.of(x * 2)).get() == 6


def test_flat_map_empty():
    assert Optional.empty().flat_map(lambda x: Optional.of(x * 2)).is_empty()


def test_flat_map_returns_empty():
    assert Optional.of(3).flat_map(lambda x: Optional.empty()).is_empty()


def test_to_stream():
    records = [
        {"city": "Paris", "temps": [12, 15, 14]},
        {"city": "Berlin", "temps": [18, 21, 19]},
        {"city": "Rome", "temps": [16, 17]},
    ]
    assert (
        Stream(records)
        .find_first(lambda r: r["city"] == "Berlin")
        .to_stream()
        .flat_map(lambda r: Stream(r["temps"]))
        .filter(lambda celsius: celsius >= 20)
        .map(lambda celsius: celsius * 9 / 5 + 32)  # to Farenheit
        .to_list()
        == [69.8]
    )


def test_to_stream_empty():
    assert Optional.empty().to_stream().to_list() == []


def test_repr_optional():
    assert repr(Optional.of(2)) == "Optional[2]"
    assert repr(Optional.of_nullable(None)) == "Optional[None]"

    assert str(Optional.of(2)) == "Optional[2]"


def test_eq():
    assert Optional.of(2) == Optional.of(2)
    assert Optional.of(2) != Optional.of(3)

    assert Optional.empty() == Optional.empty()
    assert Optional.of(2) != Optional.empty()

    with pytest.raises(TypeError) as e:
        Optional.of(2) == 2
    assert str(e.value) == "2 is not an Optional"


def test_hash():
    assert hash(Optional.of(2)) == hash(Optional.of(2))
    assert hash(Optional.empty()) == hash(Optional.empty())

    assert {Optional.of(2), Optional.of(2), Optional.empty()} == {Optional.of(2), Optional.empty()}

    with pytest.raises(TypeError) as e:
        hash(Optional.of([1, 2]))
    assert str(e.value) == "unhashable type: 'Optional' (value of type 'list' is unhashable)"

import json

import pytest

from pyrio import DictItem


def test_dict_item_map(json_dict):
    dictitem = DictItem(key="data", value=json_dict)
    assert dictitem.key == "data"
    assert dictitem.value == (
        DictItem(key="Name", value="Jennifer Smith"),
        DictItem(key="Security_Number", value=7867567898),
        DictItem(key="Phone", value="555-123-4568"),
        DictItem(key="Email", value=(DictItem(key="primary", value="jen123@gmail.com"),)),
        DictItem(key="Hobbies", value=["Reading", "Sketching", "Horse Riding"]),
        DictItem(key="Job", value=None),
    )


def test_dict_item_map_nested_dict(nested_json):
    dictitem = DictItem(key="data", value=json.loads(nested_json))
    assert dictitem.value == (
        DictItem(
            key="user",
            value=(
                DictItem(key="Name", value="John"),
                DictItem(key="Phone", value="555-123-4568"),
                DictItem(key="Security Number", value="3450678"),
            ),
        ),
        DictItem(
            key="super_user",
            value=(
                DictItem(key="Name", value="sudo"),
                DictItem(key="Email", value="admin@sudo.su"),
                DictItem(key="Some Other Number", value="000-0011"),
            ),
        ),
        DictItem(
            key="fraud",
            value=(
                DictItem(key="Name", value="Freud"),
                DictItem(key="Email", value="ziggy@psycho.au"),
            ),
        ),
    )


def test_dict_item_repr(json_dict):
    assert str(DictItem(key="data", value=json_dict)) == (
        "DictItem(key='data', value=("
        "DictItem(key='Name', value='Jennifer Smith'), "
        "DictItem(key='Security_Number', value=7867567898), "
        "DictItem(key='Phone', value='555-123-4568'), "
        "DictItem(key='Email', value=(DictItem(key='primary', value='jen123@gmail.com'),)), "
        "DictItem(key='Hobbies', value=['Reading', 'Sketching', 'Horse Riding']), "
        "DictItem(key='Job', value=None)))"
    )


def test_dict_item_eq(json_dict, nested_json):
    assert DictItem(key="data", value=json.loads(nested_json)) == DictItem(
        key="data", value=json.loads(nested_json)
    )
    assert DictItem(key="foo", value=json.loads(nested_json)) != DictItem(
        key="data", value=json.loads(nested_json)
    )
    assert DictItem(key="data", value=json_dict) != DictItem(key="data", value=json.loads(nested_json))


def test_dict_item_eq_raises(json_dict):
    nums = [1, 2, 3]
    with pytest.raises(TypeError) as e:
        DictItem(key="data", value=json_dict) == nums  # noqa
    assert str(e.value) == f"{nums} is not a DictItem"


@pytest.mark.parametrize("value", ["John", 42, (1, 2, 3), None])
def test_dict_item_hash_returns_int(value):
    assert isinstance(hash(DictItem(key="k", value=value)), int)


@pytest.mark.parametrize("value", ["John", 42, (1, 2, 3)])
def test_dict_item_hash_consistency(value):
    item = DictItem(key="k", value=value)
    other = DictItem(key="k", value=value)
    assert item == other
    assert hash(item) == hash(other)


@pytest.mark.parametrize(
    "value,expected_type",
    [([1, 2, 3], "list"), ({"a": 1}, "dict"), ({"list": [1, 2], "dict": {"a": 1}}, "dict")],
)
def test_dict_item_hash_unhashable_value_raises(value, expected_type):
    with pytest.raises(TypeError) as e:
        hash(DictItem(key="k", value=value))
    assert str(e.value) == f"unhashable type: 'DictItem' (value of type '{expected_type}' is unhashable)"


def test_dict_item_unpack():
    key, value = DictItem("a", 1)
    assert key == "a"
    assert value == 1


def test_dict_item_unpack_mapped_nested_value():
    key, value = DictItem("data", {"x": 1, "y": 2})
    assert key == "data"
    assert value == (DictItem("x", 1), DictItem("y", 2))


def test_dict_item_match():
    match DictItem("a", 1):
        case DictItem(key=k, value=v):
            assert k == "a"
            assert v == 1
        case _:
            pytest.fail("DictItem did not match")


def test_dict_item_with_key_and_value():
    item = DictItem("a", 1)
    assert item.with_key("b") == DictItem("b", 1)
    assert item.with_value(2) == DictItem("a", 2)
    assert item.with_key("b").with_value(None) == DictItem("b", None)
    assert item == DictItem("a", 1)  # original stays unchanged


def test_dict_item_entries():
    item = DictItem("user", {"Name": "Ada", "id": 1})
    assert item.entries().map(lambda x: x.key).to_list() == ["Name", "id"]
    assert item.entries().to_dict() == {"Name": "Ada", "id": 1}


def test_dict_item_entries_requires_mapping():
    with pytest.raises(TypeError) as e:
        DictItem("a", [1, 2]).entries()
    assert str(e.value) == "entries() expects a mapping value, got 'list' instead"


def test_dict_item_key_of_value_of():
    item = DictItem("a", 1)
    assert DictItem.key_of(item) == "a"
    assert DictItem.value_of(item) == 1


def test_dict_item_replace_null():
    handler = DictItem.replace_null("N/A")
    assert handler(DictItem("a", None)) == DictItem("a", "N/A")
    assert handler(DictItem("b", 0)) == DictItem("b", 0)
    assert handler(DictItem("c", "")) == DictItem("c", "")

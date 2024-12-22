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
            value=(DictItem(key="Name", value="Freud"), DictItem(key="Email", value="ziggy@psycho.au")),
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

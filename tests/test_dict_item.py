import json

import pytest

from pyrio import DictItem


class TestDictItem:
    def test_dict_item_map(self, json_dict):
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

    def test_dict_item_map_nested_dict(self, nested_json):
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

    def test_dict_item_repr(self, json_dict):
        assert str(DictItem(key="data", value=json_dict)) == (
            "DictItem(key='data', value=("
            "DictItem(key='Name', value='Jennifer Smith'), "
            "DictItem(key='Security_Number', value=7867567898), "
            "DictItem(key='Phone', value='555-123-4568'), "
            "DictItem(key='Email', value=(DictItem(key='primary', value='jen123@gmail.com'),)), "
            "DictItem(key='Hobbies', value=['Reading', 'Sketching', 'Horse Riding']), "
            "DictItem(key='Job', value=None)))"
        )

    def test_dict_item_eq(self, json_dict, nested_json):
        assert DictItem(key="data", value=json.loads(nested_json)) == DictItem(
            key="data", value=json.loads(nested_json)
        )
        assert DictItem(key="foo", value=json.loads(nested_json)) != DictItem(
            key="data", value=json.loads(nested_json)
        )
        assert DictItem(key="data", value=json_dict) != DictItem(
            key="data", value=json.loads(nested_json)
        )

    def test_dict_item_eq_raises(self, json_dict):
        nums = [1, 2, 3]
        with pytest.raises(TypeError) as e:
            DictItem(key="data", value=json_dict) == nums  # noqa
        assert str(e.value) == f"{nums} is not a DictItem"

    @pytest.mark.parametrize("value", ["John", 42, (1, 2, 3), None])
    def test_dict_item_hash_returns_int(self, value):
        assert isinstance(hash(DictItem(key="k", value=value)), int)

    @pytest.mark.parametrize("value", ["John", 42, (1, 2, 3)])
    def test_dict_item_hash_consistency(self, value):
        item = DictItem(key="k", value=value)
        other = DictItem(key="k", value=value)
        assert item == other
        assert hash(item) == hash(other)

    @pytest.mark.parametrize(
        "value,expected_type",
        [([1, 2, 3], "list"), ({"a": 1}, "dict"), ({"list": [1, 2], "dict": {"a": 1}}, "dict")],
    )
    def test_dict_item_hash_unhashable_value_raises(self, value, expected_type):
        with pytest.raises(TypeError) as e:
            hash(DictItem(key="k", value=value))
        assert (
            str(e.value)
            == f"unhashable type: 'DictItem' (value of type '{expected_type}' is unhashable)"
        )

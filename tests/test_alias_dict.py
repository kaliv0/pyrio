import pytest

from pyrio.exceptions import AliasError


def test_alias_dict(alias_dict):
    assert alias_dict[".toml"] == {"callable": "load", "import_mod": "tomli", "read_mode": "r"}
    assert (
        alias_dict[".yml"]
        == alias_dict[".yaml"]
        == {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}
    )


def test_add_alias(alias_dict):
    alias_dict.add_alias(".toml", ".tml")
    assert (
        alias_dict[".toml"]
        == alias_dict[".tml"]
        == {"callable": "load", "import_mod": "tomli", "read_mode": "r"}
    )


def test_add_multiple_aliases(alias_dict):
    alias_dict.add_alias(".json", ".jsn", ".joojoo", ".jazz")
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml", ".yml", ".jsn", ".joojoo", ".jazz"]


def test_add_alias_raises(alias_dict):
    with pytest.raises(AliasError, match="Key and corresponding alias cannot be equal: '.toml'"):
        alias_dict.add_alias(".toml", ".toml")


def test_update_alias(alias_dict):
    alias_dict.add_alias(".toml", ".yml")
    assert list(alias_dict.items()) == [
        (".json", {"callable": "load", "import_mod": "json", "read_mode": "r"}),
        (".yaml", {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}),
        (".toml", {"callable": "load", "import_mod": "tomli", "read_mode": "r"}),
        (".yml", ".toml"),
    ]


def test_update_alias_raises(alias_dict):
    with pytest.raises(KeyError, match=".foo"):
        alias_dict.add_alias(".foo", ".bar")


def test_remove_alias(alias_dict):
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml", ".yml"]
    alias_dict.remove_alias(".yml")
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml"]
    assert list(alias_dict.items()) == [
        (".json", {"callable": "load", "import_mod": "json", "read_mode": "r"}),
        (".yaml", {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}),
        (".toml", {"callable": "load", "import_mod": "tomli", "read_mode": "r"}),
    ]


def test_remove_alias_raises(alias_dict):
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml", ".yml"]
    with pytest.raises(KeyError, match=".foo"):
        alias_dict.remove_alias(".foo")


def test_remove_multiple_aliases(alias_dict):
    alias_dict.add_alias(".json", ".jsn")
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml", ".yml", ".jsn"]
    alias_dict.remove_alias(".yml", ".jsn")
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml"]


def test_read_aliases(alias_dict):
    alias_dict.add_alias(".toml", ".tml")
    alias_dict.add_alias(".json", ".jsn")
    alias_dict.add_alias(".json", ".whaaaaat!")
    assert list(alias_dict.aliases()) == [".yml", ".tml", ".jsn", ".whaaaaat!"]


def test_dictviews(alias_dict):
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml", ".yml"]
    assert list(alias_dict.values()) == [
        {"import_mod": "json", "callable": "load", "read_mode": "r"},
        {"import_mod": "yaml", "callable": "safe_load", "read_mode": "r"},
        {"import_mod": "tomli", "callable": "load", "read_mode": "r"},
    ]
    assert list(alias_dict.items()) == [
        (".json", {"callable": "load", "import_mod": "json", "read_mode": "r"}),
        (".yaml", {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}),
        (".toml", {"callable": "load", "import_mod": "tomli", "read_mode": "r"}),
        (".yml", ".yaml"),
    ]


def test_remove_key_and_aliases(alias_dict):
    assert list(alias_dict.keys()) == [".json", ".yaml", ".toml", ".yml"]
    alias_dict.pop(".yaml")
    assert list(alias_dict.keys()) == [".json", ".toml"]


def test_contains(alias_dict):
    alias_dict.add_alias(".toml", ".tml")
    assert (".toml" in alias_dict) is True
    assert (".tml" in alias_dict) is True
    assert (".foo" in alias_dict) is False


def test_get(alias_dict):
    assert alias_dict.get(".yaml") == {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}
    assert alias_dict.get(".yml") == {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}
    assert alias_dict.get(".foo") is None


def test_pop(alias_dict):
    assert alias_dict.pop(".yml") == {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}
    assert ".yaml" in alias_dict  # removing the alias doesn't remove the key


def test_iter(alias_dict):
    assert [k for k in alias_dict] == [".json", ".yaml", ".toml", ".yml"]


def test_repr(alias_dict):
    assert str(alias_dict) == (
        "AliasDict(dict_items(["
        "('.json', {'import_mod': 'json', 'callable': 'load', 'read_mode': 'r'}), "
        "('.yaml', {'import_mod': 'yaml', 'callable': 'safe_load', 'read_mode': 'r'}), "
        "('.toml', {'import_mod': 'tomli', 'callable': 'load', 'read_mode': 'r'}), "
        "('.yml', '.yaml')"
        "]))"
    )


def test_origin_keys(alias_dict):
    assert list(alias_dict.origin_keys()) == [".json", ".yaml", ".toml"]


def test_aliased_keys(alias_dict):
    assert list(alias_dict.aliased_keys()) == [(".yaml", [".yml"])]
    alias_dict.add_alias(".toml", ".tml", ".tommy", ".tomograph")
    assert list(alias_dict.aliased_keys()) == [
        (".yaml", [".yml"]),
        (".toml", [".tml", ".tommy", ".tomograph"]),
    ]

import pytest


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


def test_update_alias(alias_dict):
    alias_dict.add_alias(".toml", ".yml")
    assert alias_dict.iteritems() == (
        (".json", {"callable": "load", "import_mod": "json", "read_mode": "r"}),
        (".yaml", {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}),
        (".toml", {"callable": "load", "import_mod": "tomli", "read_mode": "r"}),
        (".yml", {"callable": "load", "import_mod": "tomli", "read_mode": "r"}),
    )


def test_update_alias_raises(alias_dict):
    with pytest.raises(KeyError, match="Key '.foo' not found"):
        alias_dict.add_alias(".foo", ".bar")


def test_remove_alias(alias_dict):
    assert alias_dict.iterkeys() == (".json", ".yaml", ".toml", ".yml")
    alias_dict.remove_alias(".yml")
    assert alias_dict.iterkeys() == (".json", ".yaml", ".toml")
    assert alias_dict.iteritems() == (
        (".json", {"callable": "load", "import_mod": "json", "read_mode": "r"}),
        (".yaml", {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}),
        (".toml", {"callable": "load", "import_mod": "tomli", "read_mode": "r"}),
    )


def test_dictviews(alias_dict):
    assert alias_dict.iterkeys() == (".json", ".yaml", ".toml", ".yml")
    assert alias_dict.itervalues() == (
        {"import_mod": "json", "callable": "load", "read_mode": "r"},
        {"import_mod": "yaml", "callable": "safe_load", "read_mode": "r"},
        {"import_mod": "tomli", "callable": "load", "read_mode": "r"},
    )
    assert alias_dict.iteritems() == (
        (".json", {"callable": "load", "import_mod": "json", "read_mode": "r"}),
        (".yaml", {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}),
        (".toml", {"callable": "load", "import_mod": "tomli", "read_mode": "r"}),
        (".yml", {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}),
    )

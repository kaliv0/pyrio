from pyrio.utils.alias_dict import AliasDict


# TODO: add more tests
def test_alias_dict():
    ad = AliasDict(
        {
            ".json": {
                "import_mod": "json",
                "callable": "load",
                "read_mode": "r",
            },
            ".yaml": {
                "import_mod": "yaml",
                "callable": "safe_load",
                "read_mode": "r",
            },
            ".toml": {
                "import_mod": "tomli",
                "callable": "load",
                "read_mode": "r",
            },
        }
    )
    ad.add_alias(".yaml", ".yml")
    assert ad[".yml"] == ad[".yaml"] == {"callable": "safe_load", "import_mod": "yaml", "read_mode": "r"}
    assert ad[".toml"] == {"callable": "load", "import_mod": "tomli", "read_mode": "r"}

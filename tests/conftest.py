import pytest

from pyrio.utils.alias_dict import AliasDict


@pytest.fixture
def Foo():
    class Foo:
        def __init__(self, name, num):
            self.name = name
            self.num = num

    return Foo


@pytest.fixture()
def alias_dict():
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
    return ad


@pytest.fixture
def nested_json():
    return """
    {
        "user": {
            "Name": "John", 
            "Phone": "555-123-4568", 
            "Security Number": "3450678"
        }, 
        "super_user": {
            "Name": "sudo", 
            "Email": "admin@sudo.su",
            "Some Other Number": "000-0011" 

        },
        "fraud": {
            "Name": "Freud", 
            "Email": "ziggy@psycho.au"
        }    
    }
    """


@pytest.fixture
def json_dict():
    return {
        "Name": "Jennifer Smith",
        "Security_Number": 7867567898,
        "Phone": "555-123-4568",
        "Email": {"primary": "jen123@gmail.com"},
        "Hobbies": ["Reading", "Sketching", "Horse Riding"],
        "Job": None,
    }


@pytest.fixture(scope="session")
def tmp_file_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("file_stream")

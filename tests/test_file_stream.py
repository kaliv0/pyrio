from pyrio.file_stream import FileStream


def test_json():
    assert FileStream("tests/resources/foo.json").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_toml():
    assert FileStream("tests/resources/bar.toml").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


def test_yaml():
    assert FileStream("tests/resources/bazz.yaml").map(lambda x: f"{x.key}=>{x.value}").to_tuple() == (
        "abc=>xyz",
        "qwerty=>42",
    )


# TODO: text .xml


def test_csv():
    assert FileStream("tests/resources/fizz.csv").map(
        lambda x: f"fizz: {x['fizz']}, buzz: {x['buzz']}"
    ).to_tuple() == (
        "fizz: 42, buzz: 45",
        "fizz: aaa, buzz: bbb",
    )


def test_nasty_json():
    assert FileStream("tests/resources/nasty.json").map(lambda x: x.value["second"]).flatten().to_list() == [
        1,
        2,
        3,
        4,
    ]

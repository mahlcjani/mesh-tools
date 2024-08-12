
from .mapper import \
    FilterChain, \
    SimpleFilter

from .basic import \
    CreateProperty, \
    Int, \
    IntProperty, \
    Float, \
    FloatProperty, \
    Replace, \
    SplitProperty, \
    Trim, \
    TrimProperty


def test_rename_property():
    result = SimpleFilter("firstName", rename_to="givenName") \
        .filter({"firstName": "Adam"})

    assert result["givenName"] == "Adam"
    assert result.get("firstName") is None


def test_create_property():
    result = CreateProperty("name", format="Jack {colors[0]}") \
        .filter({"colors": ["Black", "White"]})

    assert result["name"] == "Jack Black"


def test_split_property():
    assert SplitProperty("name").filter({"name": "alice bob"})["name"] == ["alice", "bob"]
    assert SplitProperty("name", to="names") \
        .filter({"name": "alice bob"})["names"] == ["alice", "bob"]


def test_trim_property():
    assert TrimProperty("name").filter({"name": "\t trimmed \n "})["name"] == "trimmed"


def test_int_mapping():
    data = {"age": "23"}
    # Simple case - transfer with conversion to int
    assert IntProperty("age").filter(data)["age"] == 23
    # With rename
    assert SimpleFilter("age", rename_to="years", apply=[Int()]).filter(data)["years"] == 23


def test_float_mapping():
    data = {"area": "23.45"}
    assert FloatProperty("area").filter(data)["area"] == 23.45
    # With rename
    assert SimpleFilter("area", rename_to="size", apply=[Float()]).filter(data)["size"] == 23.45


def test_replace():
    assert SimpleFilter("name", apply=[
        Trim(),
        # all ws to single space
        Replace("\\s+", " "),
        Replace(" *- *", "-"),
    ]).filter({"name": " Jan \t Duda -Grach \n"})["name"] == "Jan Duda-Grach"


def test_chain():
    properties = FilterChain([
        SimpleFilter("name", rename_to="fullname", apply=[
            Trim(),
            Replace("\\s+", " "),
            Replace(" *- *", "-"),
        ]),
        IntProperty("age")
    ]).filter({
        "name": " Jan \t Duda -Grach \n",
        "age": "38"
    })
    print(properties)
    assert properties["fullname"] == "Jan Duda-Grach"
    assert properties["age"] == 38

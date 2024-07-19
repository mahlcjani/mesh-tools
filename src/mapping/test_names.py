from unittest import TestCase
import pytest

from .names import NameMapper


@pytest.fixture
def samples():
    return [
        {
            "in": {
                "fullname": "vincent willem van gogh"
            },
            "out": {
                "firstname": "Vincent",
                "surname": "van Gogh",
                "fullname": "Vincent Willem van Gogh",
                "names": ["Vincent", "Willem"],
                "surnames": ["van Gogh"]
            }
        },
        {
            "in": {
                "fullname": "ludwig van der berg"
            },
            "out": {
                "firstname": "Ludwig",
                "surname": "van der Berg",
                "fullname": "Ludwig van der Berg",
                "names": ["Ludwig"],
                "surnames": ["van der Berg"]
            }
        },
        {
            "in": {
                "fullname": "JOE Peter SMITH vel jaMEs"
            },
            "out": {
                "firstname": "Joe",
                "surname": "Smith",
                "fullname": "Joe Peter Smith vel James",
                "names": ["Joe", "Peter"],
                "surnames": ["Smith", "James"]
            }
        },
    ]


def test_name_mapper(samples):
    mapper = NameMapper(fullname=[{"name": "fullname"}])

    for sample in samples:
        TestCase().assertDictEqual(sample["out"], mapper.map(sample["in"]))


def test_match_surname_at_start():
    match = NameMapper.match_surname_at_start("Sur-Name FirstName SecondName")
    assert match
    assert match.group("surname") == "Sur-Name"
    assert match.group("preposition") is None
    assert match.group("fullsurname") == "Sur-Name"
    assert match.string[match.end():].split() == ["FirstName", "SecondName"]


def test_match_surname_at_end():
    match = NameMapper.match_surname_at_end("FirstName SecondName van de Sur-Name")
    assert match
    assert match.group("surname") == "Sur-Name"
    assert match.group("preposition") == "van de"
    assert match.group("fullsurname") == "van de Sur-Name"
    assert match.string[0:match.start()].split() == ["FirstName", "SecondName"]

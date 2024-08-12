from unittest import TestCase
import pytest

from .mapper import FilterChain
from .names import FullnameBuilder, FullnameFilter, match_surname_at_end, match_surname_at_start


@pytest.fixture
def samples():
    return [
        {
            "in": {
                "name": "vincent willem van gogh"
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
                "name": "ludwig van der berg"
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
                "name": "JOE Peter SMITH vel jaMEs"
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


def test_fullname_filter(samples):
    filter = FilterChain([
        FullnameFilter(
            "name",
            surname_at_end=True,
            names="names",
            firstname="firstname",
            surnames="surnames",
            surname="surname"
        ),
        FullnameBuilder("fullname")
    ])

    for sample in samples:
        TestCase().assertDictEqual(sample["out"], filter.filter(sample["in"]))


def test_match_surname_at_start():
    match = match_surname_at_start("Sur-Name FirstName SecondName")
    assert match
    assert match.group("surname") == "Sur-Name"
    assert match.group("preposition") is None
    assert match.group("fullsurname") == "Sur-Name"
    assert match.string[match.end():].split() == ["FirstName", "SecondName"]


def test_match_surname_at_end():
    match = match_surname_at_end("FirstName SecondName van de Sur-Name")
    assert match
    assert match.group("surname") == "Sur-Name"
    assert match.group("preposition") == "van de"
    assert match.group("fullsurname") == "van de Sur-Name"
    assert match.string[0:match.start()].split() == ["FirstName", "SecondName"]

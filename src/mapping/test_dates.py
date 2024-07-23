from unittest import TestCase
import pytest

from .dates import DateMapper


@pytest.fixture
def samples():
    return [
        {
            "in": {
                "dateField": "1991-01-23"
            },
            "out": {
                "date": "1991-01-23",
            }
        },
        {
            "in": {
                "Data": "23-01-1991"
            },
            "out": {
                "date": "1991-01-23",
            }
        },
        {
            "in": {
                "Data": "1991-12-25"
            },
            "out": {
                "date": "1991-12-25",
            }
        },
    ]


def test_name_mapper(samples):
    mapper = DateMapper(date=["dateField", "Data"])

    for sample in samples:
        TestCase().assertDictEqual(sample["out"], mapper.map(sample["in"]))

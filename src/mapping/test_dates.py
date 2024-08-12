from unittest import TestCase
import pytest

from .dates import DateFilter
from .mapper import FilterChain


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


def test_date_filter(samples):
    filter = FilterChain([
        DateFilter("dateField", name="date"),
        DateFilter("Data", name="date")
    ])

    for sample in samples:
        TestCase().assertDictEqual(sample["out"], filter.filter(sample["in"]))

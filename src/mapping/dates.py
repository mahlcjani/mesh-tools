import re
from datetime import date
from typing import Any, Self

from .mapper import Properties, PropertiesFilter
from ..dates import fromisoformat, fromplformat


class DateFilter(PropertiesFilter):

    @staticmethod
    def sanitize_date(date_string: str) -> str:
        # - remove spaces around dash (-)
        # - strip leading and trailing spaces
        return re.sub(" *- *", "-", date_string).strip()

    @staticmethod
    def try_fromisoformat(date_string: str) -> date | None:
        try:
            return fromisoformat(date_string)
        except ValueError:
            return None

    @staticmethod
    def try_fromplformat(date_string: str) -> date | None:
        try:
            return fromplformat(date_string)
        except ValueError:
            return None

    def __init__(self, property: str, **kwargs: list[Any]) -> None:
        self._name = property
        self._out_name = kwargs["name"]

    def filter(self: Self, data: Properties) -> Properties:
        field = data.pop(self._name, None)
        if field:
            date_string = DateFilter.sanitize_date(field)
            date_attr = DateFilter.try_fromisoformat(date_string)
            if not date_attr:
                date_attr = DateFilter.try_fromplformat(date_string)

            return {
                self._out_name: date_attr.isoformat() if date_attr else field
            }

        return {}

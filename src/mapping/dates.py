import re
from datetime import date
from typing import Any, Self

from .mapper import Attributes, Mapper
from ..dates import fromisoformat, fromplformat


class DateMapper(Mapper):

    @staticmethod
    def sanitize_date(date_string: str) -> str:
        # - remove spaces around dash (-)
        # - strip leading and trailing spaces
        return re.sub(" *- *", "-", date_string).strip()

    @staticmethod
    def fromisoformat(date_string: str) -> date | None:
        try:
            return fromisoformat(date_string)
        except:
            return None

    @staticmethod
    def fromplformat(date_string: str) -> date | None:
        try:
            return fromplformat(date_string)
        except:
            return None

    def __init__(self, **kwargs: list[Any]) -> None:
        self.__date_rules = kwargs

    def map(self: Self, data: Attributes) -> Attributes:
        properties = {}

        for out, fields in self.__date_rules.items():
            for field in fields:
                if field in data:
                    date_string = DateMapper.sanitize_date(data[field])
                    date_attr = DateMapper.fromisoformat(date_string)
                    if not date_attr:
                        date_attr = DateMapper.fromplformat(date_string)

                    if date_attr:
                        properties[out] = date_attr.isoformat()
                        break

        return properties

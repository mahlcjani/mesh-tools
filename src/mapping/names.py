
from ..types import ImportedPerson
from ..dates import fromisoformat, fromplformat

from typing import Any, Match, Self

import pycasestyle
import re

class Mapper:
    def map(self: Self, data: dict[str, Any]) -> dict[str, Any]:
        return {}

class CompositeMapper(Mapper):
    def __init__(self, mappers: list[Mapper], keep_input: bool = False, input_node: str = "") -> None:
        self.__mappers = mappers
        self.__keep_input = keep_input
        self.__input_node = input_node

    def map(self: Self, data: dict[str, Any]) -> dict[str, Any]:
        properties = {} if not self.__keep_input else { self.__input_node: data } if self.__input_node else data.copy()

        for mapper in self.__mappers:
            properties.update(mapper.map(data))

        return properties

class BirthdateMapper(Mapper):

    @staticmethod
    def sanitize_date(str: str) -> str:
        # - remove spaces around dash (-)
        # - strip leading and trailing spaces
        return re.sub(" *- *", "-", str).strip()

    def __init__(self, **kwargs: list[Any]) -> None:
        self.__birthdate = kwargs.get("birthdate", [])

    def map(self: Self, data: dict[str, Any]) -> dict[str, Any]:
        birth_date = None

        for key in self.__birthdate:
            if key["name"] in data:
                datestr = BirthdateMapper.sanitize_date(data[key["name"]])
                birth_date = fromisoformat(datestr)
                if not birth_date:
                    birth_date = fromplformat(datestr)

            if birth_date:
                break

        return { "birthdate": birth_date.isoformat() } if birth_date else {}

class NameMapper(Mapper):

    # Mind the order!
    prepositions = ["de", "da", "di", "von", "van der", "van de", "van"]

    surname_at_start = re.compile(f"^(?P<fullsurname>((?P<preposition>{'|'.join(prepositions)})? )?(?P<surname>[\\w-]+))", re.IGNORECASE)
    surname_at_end = re.compile(f"(?P<fullsurname>((?P<preposition>{'|'.join(prepositions)})? )?(?P<surname>[\\w-]+))$", re.IGNORECASE)

    @staticmethod
    def match_surname_at_start(name: str) -> Match[str] | None:
        return NameMapper.surname_at_start.search(name)

    @staticmethod
    def match_surname_at_end(name: str) -> Match[str] | None:
        return NameMapper.surname_at_end.search(name)

    @staticmethod
    def sanitize_name(str: str) -> str:
        # - replace all repeated white spaces with single space
        # - remove spaces around dash (-) - name joiner
        # - strip leading and trailing spaces
        return re.sub(" *- *", "-", re.sub("\\s+", " ", str)).strip()

    # use jq '.[] | keys' on json to get keys
    def __init__(self, **kwargs: list[Any]) -> None:
        self.__names = kwargs.get("names", [])
        self.__surname = kwargs.get("surname", [])
        self.__fullname = kwargs.get("fullname", [])

    def map(self: Self, data: dict[str, Any]) -> dict[str, Any]:
        properties = {}

        for key in self.__fullname:
            if key["name"] in data:
                fullname = NameMapper.sanitize_name(data[key["name"]])
                extract_names = bool(key.get("extract_names", True))
                surname_at_end = key.get("surname_at_end", True)
                properties.update(self.parse_fullname(fullname, extract_names, surname_at_end))
                # For now first found key wins
                break

        for key in self.__names:
            if key["name"] in data:
                properties.update(self.parse_names(NameMapper.sanitize_name(data[key["name"]])))
                break

        for key in self.__surname:
            if key["name"] in data:
                properties.update(self.parse_surnames(NameMapper.sanitize_name(data[key["name"]])))
                break

        return properties

    def parse_fullname_1(self: Self, fullname: str, surname_at_end: bool) -> dict[str, str]:
        names = []

        if surname_at_end:
            # name [name...] [preposition] surname
            match = NameMapper.match_surname_at_end(fullname)
            if not match:
                return { "fullname": fullname }

            names = match.string[0:match.start()].split()
        else:
            # [preposition] surname name [name...]
            match = NameMapper.match_surname_at_start(fullname)
            if not match:
                return { "fullname": fullname }

            names = match.string[match.end():].split()

        surname = match.group("surname").capitalize()
        preposition = match.group("preposition")
        if preposition:
            surname = preposition.casefold() + " " + surname

        names = list(map(lambda str: str.capitalize(), names))

        return {
            "fullname": " ".join(names) + " " + surname,
            "names": names,
            "firstname": names[0],
            "surname": surname,
            "surnames": [surname],
        }

    def parse_fullname(self: Self, fullname: str, extract_names: bool, surname_at_end: bool) -> dict[str, str]:
        if not extract_names:
            return {
                "fullname": fullname
            }

        parts = re.split(" vel ", fullname, flags=re.IGNORECASE)

        properties = {}

        if surname_at_end:
            properties.update(self.parse_fullname_1(parts[0], surname_at_end))
            for surname in parts[1:]:
                properties["surnames"].append(self.parse_surname(surname)["surname"])
        else:
            properties.update(self.parse_fullname_1(parts[-1], surname_at_end))
            for surname in parts[0:-1]:
                properties["surnames"].append(self.parse_surname(surname)["surname"])

        surnames = properties["surnames"]
        if len(surnames) > 1:
            properties.update({
                "fullname": " ".join(properties["names"]) + " " + " vel ".join(surnames)
            })

        return properties

    def parse_names(self: Self, names: str) -> dict[str, str]:
        parts = list(map(lambda name: name.capitalize(), names.split()))
        return {
            "names": parts,
            "firstname": parts[0]
        }

    def parse_surname(self: Self, name: str) -> dict[str, str]:
        parts = []

        # We use surname matching regex to get preposition, which will be lowered
        # as opposed to other parts, which will be capitalized
        match = NameMapper.match_surname_at_start(name)
        if match and match.group("preposition"):
            preposition = match.group("preposition").casefold()
            name = name[len(preposition)+1:]
            parts.append(preposition)

        parts.extend(list(map(lambda str: str.capitalize(), name.split())))
        surname = " ".join(parts)
        return {
            "surname": surname,
            "surnames": [surname]
        }

    def parse_surnames(self: Self, name: str) -> dict[str, str]:
        parts = re.split(" vel ", name, flags=re.IGNORECASE)
        surnames = []
        for surname in parts:
            surnames.append(self.parse_surname(surname)["surname"])

        return {
            "surnames": surnames,
            "surname": surnames[0]
        }

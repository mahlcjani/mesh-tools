import re
from typing import Any, Match, Self, Tuple
from .mapper import Properties, PropertiesFilter


def sanitize_name(name: str) -> str:
    # - replace all repeated white spaces with single space
    # - remove spaces around dash (-) - name joiner
    # - strip leading and trailing spaces
    return re.sub(" *- *", "-", re.sub("\\s+", " ", name)).strip()


def capitalize_name(name: str) -> str:
    return "-".join([s.capitalize() for s in name.split("-")])


def capitalize_names(names: str) -> list[str]:
    return [capitalize_name(name) for name in [s for s in names.split()]]


# Mind the order!
prepositions = ["da", "de", "di", "van der", "van de", "van", "von"]

surname_at_start = re.compile(
    f"^(?P<fullsurname>((?P<preposition>{'|'.join(prepositions)})? )?(?P<surname>[\\w-]+))",
    re.IGNORECASE
)

surname_at_end = re.compile(
    f"(?P<fullsurname>((?P<preposition>{'|'.join(prepositions)})? )?(?P<surname>[\\w-]+))$",
    re.IGNORECASE
)


def match_surname_at_start(name: str) -> Match[str] | None:
    return surname_at_start.search(name)


def match_surname_at_end(name: str) -> Match[str] | None:
    return surname_at_end.search(name)


class NamesFilter(PropertiesFilter):
    """Filter string names into firstname and array of names"""

    def __init__(self: Self, name: str, **kwargs: str) -> None:
        # property name to parse
        self._name = name
        # properties to write
        # - parsed names
        self._names = kwargs.get("names", "names")
        # - parsed firstname
        self._firstname = kwargs.get("firstname", "firstname")

    def filter(self: Self, data: Properties) -> Properties:
        name = data.pop(self._name, None)
        if not name:
            return {}

        names = self.parse_names(sanitize_name(name))
        return {
            self._names: names,
            self._firstname: names[0],
        }

    def parse_names(self: Self, names: str) -> list[str]:
        return capitalize_names(names)


class SurnameFilter(PropertiesFilter):
    """Filter surname into surname and array of surnames properties"""

    def __init__(self: Self, name: str, **kwargs: str) -> None:
        # property name to parse
        self._name = name
        # properties to write
        # - parsed surnames
        self._surnames = kwargs.get("surnames", "surnames")
        # - parsed main surname
        self._surname = kwargs.get("surname", "surname")

    def filter(self: Self, data: Properties) -> Properties:
        name = data.pop(self._name, None)
        if not name:
            return {}

        surnames = self.parse_surnames(sanitize_name(name))
        return {
            self._surname: surnames[0],
            self._surnames: surnames,
        }

    def parse_surname(self: Self, name: str) -> str:
        parts = []

        # We use surname matching regex to get preposition,
        # which will be lowered as opposed to other parts,
        # which will be capitalized
        match = match_surname_at_start(name)
        if match and match.group("preposition"):
            preposition = match.group("preposition").casefold()
            name = name[len(preposition)+1:]
            parts.append(preposition)

        parts.extend(capitalize_names(name))

        return " ".join(parts)

    def parse_surnames(self: Self, name: str) -> list[str]:
        return list(map(self.parse_surname, re.split(" vel ", name, flags=re.IGNORECASE)))


class FullnameFilter(PropertiesFilter):
    """
        Filter person's full name into component (names, firstname, surnames).
        Seems jq alone and with functions is impossible to do this job.
    """

    # use jq '.[] | keys' on json to get keys
    def __init__(self: Self, name: str, **kwargs: Any) -> None:
        # property name to parse
        self._name = name
        # is surname at the end of parsed proprty
        self._surname_at_end = kwargs.get("surname_at_end", True)
        # properties to write
        # - normalized full name
        self._fullname = kwargs.get("fullname", "fullname")
        # - parsed names
        self._names = kwargs.get("names", "names")
        # - parsed firstname
        self._firstname = kwargs.get("firstname", "firstname")
        # - parsed surnames
        self._surnames = kwargs.get("surnames", "surnames")
        # - parsed main surname
        self._surname = kwargs.get("surname", "surname")

        self._names_mapper = NamesFilter(None, firstname=self._firstname, names=self._names)
        self._surname_mapper = SurnameFilter(None, surname=self._surname, surnames=self._surnames)

    def filter(self: Self, data: Properties) -> Properties:
        fullname = data.pop(self._name, None)
        if not fullname:
            return {}

        (surnames, names) = self.parse_fullname(sanitize_name(fullname))

        properties = {}

        if len(names):
            properties.update({
                self._names: names,
                self._firstname: names[0],
            })

        if len(surnames):
            properties.update({
                self._surname: surnames[0],
                self._surnames: surnames,
            })

        return properties

    def parse_fullname_1(self: Self, fullname: str) -> Tuple[str, list[str]]:
        names = []

        if self._surname_at_end:
            # name [name...] [preposition] surname
            match = match_surname_at_end(fullname)
            if not match:
                return ([], [])

            names = match.string[0:match.start()]
        else:
            # [preposition] surname name [name...]
            match = match_surname_at_start(fullname)
            if not match:
                return ([], [])

            names = match.string[match.end():]

        surname = capitalize_name(match.group("surname"))
        preposition = match.group("preposition")
        if preposition:
            surname = preposition.casefold() + " " + surname

        return (surname, self._names_mapper.parse_names(names))

    # pylint: disable=C0301
    def parse_fullname(self: Self, fullname: str) -> Tuple[list[str], list[str]]:
        parts = re.split(" vel ", fullname, flags=re.IGNORECASE)

        surnames = []
        names = []

        if self._surname_at_end:
            (s, n) = self.parse_fullname_1(parts[0])
            surnames.append(s)
            names.extend(n)
            for surname in parts[1:]:
                surnames.append(self._surname_mapper.parse_surname(surname))
        else:
            (s, n) = self.parse_fullname_1(parts[-1])
            surnames.append(s)
            names.extend(n)
            for surname in parts[0:-1]:
                surnames.append(self._surname_mapper.parse_surname(surname))

        return (surnames, names)

    def parse_surname(self: Self, name: str) -> Properties:
        parts = []

        # We use surname matching regex to get preposition,
        # which will be lowered as opposed to other parts,
        # which will be capitalized
        match = match_surname_at_start(name)
        if match and match.group("preposition"):
            preposition = match.group("preposition").casefold()
            name = name[len(preposition)+1:]
            parts.append(preposition)

        parts.extend(capitalize_names(name))
        surname = " ".join(parts)
        return {
            "surname": surname,
            "surnames": [surname]
        }

    def parse_surnames(self: Self, name: str) -> Properties:
        parts = re.split(" vel ", name, flags=re.IGNORECASE)
        surnames = []
        for surname in parts:
            surnames.append(self.parse_surname(surname)["surname"])

        return {
            "surnames": surnames,
            "surname": surnames[0]
        }


class FullnameBuilder(PropertiesFilter):
    """Format person's full name from names and surnames."""

    # use jq '.[] | keys' on json to get keys
    def __init__(self: Self, name: str, **kwargs: Any) -> None:
        # property name to create
        self._name = name
        self._names = kwargs.get("names", "names")
        self._surnames = kwargs.get("surnames", "surnames")

    def filter(self: Self, data: Properties) -> Properties:
        names = data.get(self._names)
        surnames = data.get(self._surnames)
        # It seems to make sense to create full name only if both
        # names and surnames are not empty
        return {
            self._name: " ".join(names) + " " + " vel ".join(surnames)
        } if len(names) and len(surnames) else {}

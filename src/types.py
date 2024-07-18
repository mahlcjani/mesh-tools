
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Self, cast

type Metadata = dict[str, str]

class Person:

    def __init__(self, **kwargs: str|date) -> None:
        assert "firstname" in kwargs, "firstname is a mandatory argument"
        assert "surname" in kwargs, "surname is a mandatory argument"

        self.firstname = cast(str, kwargs["firstname"])
        self.surname = cast(str, kwargs["surname"])
        self.fullname = cast(str, kwargs.get("fullname", f"{self.firstname} {self.surname}"))
        self.birthdate = cast(date, kwargs.get("birthdate"))
        self.__metadata = {}

    def __repr__(self: Self) -> str:
        return f"{self.__class__.__name__}({self.fullname}) at {hex(id(self))}"

    @property
    def firstname(self: Self) -> str:
        return self.__firstname

    @firstname.setter
    def firstname(self: Self, firstname: str) -> None:
        self.__firstname = firstname

    @property
    def surname(self: Self) -> str:
        return self.__surname

    @surname.setter
    def surname(self: Self, surname: str) -> None:
        self.__surname = surname

    @property
    def fullname(self: Self) -> str:
        return self.__fullname

    @fullname.setter
    def fullname(self: Self, fullname: str) -> None:
        self.__fullname = fullname

    @property
    def birthdate(self: Self) -> str:
        return self.__birthdate

    @birthdate.setter
    def birthdate(self: Self, birthdate: date) -> None:
        self.__birthdate = birthdate

    @property
    def metadata(self: Self) -> Metadata:
        return self.__metadata


class PersistedPerson(Person):
    """Person already existing in the system"""

    def __init__(self, id: str, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.__id = id

    def __repr__(self: Self) -> str:
        return f"{self.__class__.__name__}({self.fullname}/{self.__id}) at {hex(id(self))}"

    @property
    def id(self) -> str:
        return self.__id

class ImportedPerson(Person):
    """Imported person"""

    @property
    def resolved(self) -> Any:
        return self.__resolved

    @resolved.setter
    def resolved(self, r: Any) -> None:
        assert r.source == self
        self.__resolved = r

class ResolvedPerson:

    def __init__(self, source: ImportedPerson, target: str):
        self.__source = source
        self.__target = target
        self.__source.resolved = self

    def __repr__(self: Self) -> str:
        return f"{self.__class__.__name__}({self.source.fullname}/{self.__target}) at {hex(id(self))}"

    @property
    def source(self: Self) -> ImportedPerson:
        return self.__source

    @property
    def target(self: Self) -> str:
        return self.__target

class ResolvedExistingPerson(ResolvedPerson):
    def __init__(self, source: ImportedPerson, target: PersistedPerson):
        super().__init__(source, target.id)
        self.__targetPerson = target

    @property
    def targetPerson(self: Self) -> PersistedPerson:
        return self.__targetPerson

class PersonId:
    pass

class PersonResolver:
    def find_person(self: Self, person: ImportedPerson) -> PersistedPerson | None:
        pass

    def resolve(self: Self, person: ImportedPerson) -> ResolvedPerson:
        # create person id from person properties
        # find that person using the id
        # or find person using properties and return its id
        record = self.find_person(person)
        if record:
            return ResolvedExistingPerson(person, record)

        # we may find similar persons, maybe we do not have


        return ResolvedPerson(person, id)

    def persist(self: Self, person: ResolvedPerson) -> ResolvedExistingPerson:

        return ResolvedExistingPerson(
            person.source,
            PersistedPerson(
                "genid",
                firstname=person.source.firstname,
                surname=person.source.surname,
                fullname=person.source.fullname
            )
        )

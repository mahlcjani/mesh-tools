from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, kw_only=True)
class ChamberTerm:
    chamber: str
    term: int
    name: str
    long_name: str
    start_date: date
    end_date: date


@dataclass(frozen=True, kw_only=True)
class ParlimentaryGroup:
    name: str
    desc: str


@dataclass(frozen=True, kw_only=True)
class Person:
    #name: str
    first_name: str
    surname: str
    middle_name: str = None
    birth_date: date = None
    names: list[str] = None


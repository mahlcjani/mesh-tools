from datetime import date, datetime

__PLFORMAT = "%d-%m-%Y"


def fromisoformat(date_string: str) -> date:
    return datetime.fromisoformat(date_string).date()


def isoformat(dt: date) -> str:
    return dt.isoformat()


def fromplformat(date_string: str) -> date:
    return datetime.strptime(date_string, __PLFORMAT).date()


def plformat(dt: date) -> str:
    return date.strftime(dt, __PLFORMAT)


def frompesel(pesel: str) -> date:
    # TODO - assert length and all digits
    assert len(pesel) == 11
    year = int(pesel[0:2])
    month = int(pesel[2:4])
    day = int(pesel[4:6])
    return date(1900+year, month, day) if month < 13 else date(2000+year, month-20, day)

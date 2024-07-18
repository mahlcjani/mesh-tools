from datetime import date, datetime

__plformat = "%d-%m-%Y"

def fromisoformat(str: str) -> date:
    return datetime.fromisoformat(str).date()

def isoformat(date: date) -> str:
    return date.isoformat()

def fromplformat(str: str) -> date:
    return datetime.strptime(str, __plformat).date()

def plformat(date: date) -> str:
    return date.strftime(__plformat)

def frompesel(pesel: str) -> date:
    # naive - assert length and all digits
    assert len(pesel) == 11
    year = int(pesel[0:2])
    month = int(pesel[2:4])
    day = int(pesel[4:6])
    return date(1900+year, month, day) if month < 13 else date(2000+year, month-20, day)

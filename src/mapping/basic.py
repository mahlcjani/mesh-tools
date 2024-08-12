
import re
from typing import Any, Self
from .mapper import Properties, PropertiesFilter, ValueMapper


class CopyProperty(PropertiesFilter):
    """Copy property"""

    def __init__(self: Self, name: str, **kwargs: str) -> None:
        self._name = name
        self._to = kwargs.get("rename_to", self._name)

    def filter(self: Self, data: Properties) -> Properties:
        property = data.get(self._name)
        return {
            data[self._to]: property
        } if property else {}


class CreateProperty(PropertiesFilter):
    """Create property"""

    def __init__(self: Self, name: str, **kwargs: str) -> None:
        self._name = name
        self._format = kwargs["format"]

    def filter(self: Self, data: Properties) -> Properties:
        return {
            self._name: self._format.format(**data)
        }


class SplitProperty(PropertiesFilter):
    """Convert property to an array"""

    def __init__(self: Self, name: str, **kwargs: str) -> None:
        self._name = name
        self._to = kwargs.get("to", self._name)

    def filter(self: Self, data: Properties) -> Properties:
        property = data.get(self._name)
        return {
            self._to: property.split()
        } if property else {}


class TrimProperty(PropertiesFilter):
    """Remove white spaces from both ends of string property"""

    def __init__(self: Self, name: str) -> None:
        self._name = name

    def filter(self: Self, data: Properties) -> Properties:
        property = data.get(self._name)
        return {
            self._name: property.strip()
        } if property else {}


class IntProperty(PropertiesFilter):
    """Convert property to int"""

    def __init__(self: Self, name: str) -> None:
        self._name = name

    def filter(self: Self, data: Properties) -> Properties:
        property = data.get(self._name)
        return {
            self._name: int(property)
        } if property else {}


class FloatProperty(PropertiesFilter):
    """Convert property to float"""

    def __init__(self: Self, name: str) -> None:
        self._name = name

    def filter(self: Self, data: Properties) -> Properties:
        property = data.get(self._name)
        return {
            self._name: float(property)
        } if property else {}


class Replace(ValueMapper):
    def __init__(self: Self, pattern: str, repl: str = "") -> None:
        self._pattern = re.compile(pattern)
        self._repl = repl

    def map(self: Self, value: Any) -> Any:
        return re.sub(self._pattern, self._repl, str(value))


class Trim(ValueMapper):
    def map(self: Self, value: Any) -> Any:
        return str(value).strip()


class Capitalize(ValueMapper):
    def map(self: Self, value: Any) -> Any:
        return str(value).capitalize()


class Int(ValueMapper):
    def map(self: Self, value: Any) -> Any:
        return int(value)


class Float(ValueMapper):
    def map(self: Self, value: Any) -> Any:
        return float(value)

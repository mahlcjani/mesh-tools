
from functools import reduce
from typing import Any, Protocol, Self


# Properties is just an alias for dictionary
type Properties = dict[str, Any]


class PropertiesFilter(Protocol):
    """Transform dictionary from one format to another"""

    def filter(self: Self, data: Properties) -> Properties:
        pass


class ValueMapper(Protocol):
    """Convert value to another value"""

    def map(self: Self, value: Any) -> Any:
        pass


class SimpleFilter(PropertiesFilter):
    """Pick a property and apply value mapping"""

    # def __init__(self: Self, name: str, mappers: list[ValueMapper], to: str) -> None:
    def __init__(self: Self, name: str, **kwargs: Any) -> None:
        self._name = name
        self._rename_to = kwargs.get("rename_to", name)
        self._mappers = kwargs.get("apply", [])

    def filter(self: Self, data: Properties) -> Properties:
        property = data.get(self._name)
        return {
            self._rename_to: reduce(lambda v, m: m.map(v), self._mappers, property)
        } if property else {}


class FilterChain(PropertiesFilter):
    """Simple filter act on single property, this chain allows for filtering more properties."""

    def __init__(self: Self, filters: list[PropertiesFilter]) -> None:
        self._filters = filters

    def filter(self: Self, data: Properties) -> Properties:
        properties = data
        for filter in self._filters:
            properties.update(filter.filter(properties))
        return properties

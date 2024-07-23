from typing import Any, Protocol, Self


# Attributes are just an alias for dictionary
type Attributes = dict[str, Any]


class Mapper(Protocol):
    """Transform dictonary from one format to another"""
    def map(self: Self, data: Attributes) -> Attributes:
        pass


class CompositeMapper(Mapper):
    def __init__(self, mappers: list[Mapper], keep_input: bool = False, input_node: str = "") -> None:
        self.__mappers = mappers
        self.__keep_input = keep_input
        self.__input_node = input_node

    def map(self: Self, data: dict[str, Any]) -> dict[str, Any]:
        properties = {} if not self.__keep_input \
            else {self.__input_node: data} if self.__input_node \
            else data.copy()

        for mapper in self.__mappers:
            properties.update(mapper.map(data))

        return properties

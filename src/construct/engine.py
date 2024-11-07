import json

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, Self


@dataclass(frozen=True)
class ObjectId:
    type: str
    id: str


@dataclass(frozen=True)
class ObjectKeys:
    type: str
    keys: dict[str, str | int | float]

# TODO: join ObjectId into ObjectKeys -> keys: str | dict[...]


class Contains(ObjectKeys):
    def __init__(self: Self, type: str, name: str, value: str | int | float) -> None:
        super().__init__(type, {name: value})

    def get(self: Self) -> tuple[str, str | int | float]:
        return list(self.keys.items())[0]


# Data we store - dictionary of everything (for simplicity)
type Object = dict[str, Any]
# Object that is stored
type PersistedObject = tuple[ObjectId, Object]
# Object that is Linked to other objects
type LinkedObject = tuple[PersistedObject, list[PersistedObject]]


class Storage(Protocol):
    """Backend, object storage"""

    def match(self: Self, keys: ObjectId | ObjectKeys) -> ObjectId | None:
        """"Verify existance of unique object of given ID or keys"""
        return None

    # match_all
    # match_any

    def create(self: Self, type: str, name: str, data: Object) -> ObjectId:
        """Create new object"""
        pass

    def merge(self: Self, type: str, name: str, data: Object) -> ObjectId:
        """Create or update object if such exists (using name as the key)"""
        pass

    def merge_objects(self: Self, nodelist: list[ObjectId]) -> ObjectId:
        """Merge existing objects into new one"""
        pass

    def join(self: Self, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        pass

    def find_duplicates(
        self: Self,
        type: str,
        callback: Callable[[str, int, list[LinkedObject]], None] | None = None
    ) -> list[tuple[str, int, list[LinkedObject]]] | None:
        """
        Find all names that are duplicated of given object type.

        This method can be called in two flavours, depending if called with callback arg.
        In such a case callback will be called for each duplication found and no result will
        be returned. Otherwise (default) list of duplicates will be returned.

        Return (and callback args) consists of
         duplicated name,
         number of name duplicates,
         duplicated object
         and object linked with it.
        """
        pass


class DataEngine:
    """Backend facade"""

    def __init__(self: Self, storage: Storage) -> None:
        self.storage = storage

    def match(self: Self, keys: ObjectId | ObjectKeys) -> ObjectId:
        return self.storage.match(keys)

    def insert(self: Self, type: str, name: str, data: Object = dict()) -> ObjectId:
        return self.storage.create(type, name, data)

    def upsert(self: Self, type: str, name: str, data: Object = dict()) -> ObjectId:
        return self.storage.merge(type, name, data)

    def merge(self: Self, nodelist: list[ObjectId]) -> ObjectId:
        return self.storage.merge_nodes(nodelist)

    def join(self: Self, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        """Create whole-part (parent-child) relation"""
        self.storage.join(whole, part)

#    def join(self: Self, whole: ObjectId | ObjectKeys, type: str, name: str, data: Any = dict()) -> ObjectId:
#        self.storage.join(whole, part)

    def find_duplicates(
        self: Self,
        type: str,
        callback: Callable[[str, int, list[LinkedObject]], None] | None = None
    ) -> list[tuple[str, int, list[LinkedObject]]] | None:
        return self.storage.find_duplicates(type, callback)

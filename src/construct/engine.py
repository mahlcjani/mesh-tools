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


class Storage(Protocol):
    """To abstract storage and relation types"""

    # def __init__(self: Self, namespace: str) -> None:
    #     self.namespace = namespace

    def init_namespace(self: Self, namespace: str, clean: bool = False) -> None:
        pass

    def match(self: Self, namespace: str, keys: ObjectId | ObjectKeys) -> ObjectId:
        """"Verify existance of unique object of given ID or keys in context of specified namespace"""
        return None

    # match_all
    # match_any

    def create(self: Self, namespace: str, type: str, name: str, data: Any) -> ObjectId:
        pass

    def merge(self: Self, namespace: str, type: str, name: str, data: Any) -> ObjectId:
        pass

    def join(self: Self, namespace: str, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        pass


class DataEngine:
    """

    """
    def __init__(self: Self, storage: Storage) -> None:
        self.storage = storage
        # Default namespace
        self.namespace = ""

    def use_namespace(self: Self, name: str, erase: bool = False) -> Self:
        self.storage.init_namespace(name, erase)
        engine = DataEngine(self.storage)
        engine.namespace = name
        return engine

    def match(self: Self, keys: ObjectId | ObjectKeys) -> ObjectId:
        return ObjectId(keys.type, self.storage.match(self.namespace, keys))

    def insert(self: Self, type: str, name: str, data: Any) -> ObjectId:
        print("insert", type, name)
        return ObjectId(type, self.storage.create(self.namespace, type, name, data))

    def upsert(self: Self, type: str, name: str, data: Any) -> ObjectId:
        print("upsert", type, name)
        return ObjectId(type, self.storage.merge(self.namespace, type, name, data))

    def join(self: Self, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        """Create whole-part (parent-child) relation"""
        print("join", whole, part)
        self.storage.join(self.namespace, whole, part)

    def prepare(self: Self, file: str) -> None:
        """Prepare to merge namespace into default namespace,
          creates file for conflict resolution"""
        pass

    def resolve(self: Self, file: str) -> None:
        """Resolve conficts interactively """
        pass

    def merge(self: Self, file: str) -> None:
        """Merge namespaces after conflicts are resolved"""
        pass


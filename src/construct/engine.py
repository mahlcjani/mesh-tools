import json

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

    def match(self: Self, keys: ObjectId | ObjectKeys) -> ObjectId:
        """"Verify existance of unique object of given ID or keys"""
        return None

    # match_all
    # match_any

    def create(self: Self, type: str, name: str, data: Any) -> ObjectId:
        """Create new node"""
        pass

    def merge(self: Self, type: str, name: str, data: Any) -> ObjectId:
        """Create or update node if such exists (using name as the key)"""
        pass

    def merge_nodes(self: Self, nodelist: list[ObjectId]) -> ObjectId:
        """Merge existing nodes into new one"""
        pass

    def join(self: Self, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        pass

    def find_duplicates(self: Self, type: str) -> list[dict[str, Any]]:
        pass


class DataEngine:
    """

    """
    def __init__(self: Self, storage: Storage) -> None:
        self.storage = storage

    def match(self: Self, keys: ObjectId | ObjectKeys) -> ObjectId:
        return ObjectId(keys.type, self.storage.match(keys))

    def insert(self: Self, type: str, name: str, data: Any = dict()) -> ObjectId:
        return self.storage.create(type, name, data)

    def upsert(self: Self, type: str, name: str, data: Any = dict()) -> ObjectId:
        return self.storage.merge(type, name, data)

    def merge(self: Self, nodelist: list[ObjectId]) -> ObjectId:
        return self.storage.merge_nodes(nodelist)

    def join(self: Self, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        """Create whole-part (parent-child) relation"""
        self.storage.join(whole, part)

#    def join(self: Self, whole: ObjectId | ObjectKeys, type: str, name: str, data: Any = dict()) -> ObjectId:
#        self.storage.join(whole, part)

    def report_duplicates(self: Self, type: str) -> None:
        """Prepare to merge nodes of the same names"""

        last_name = None

        def callback(node_id, node, memberships):
            nonlocal last_name
            name = node.get("name")
            birth_year = node.get("birthYear")
            domicile = node.get("domicile")
            profession = node.get("profession")
            if not last_name or name != last_name:
                if last_name:
                    print()
                    print("---")
                    print()
                print(name)
                print()
                last_name = name

            print("?", node_id)
            print(name, birth_year if birth_year else "")
            print(", ".join(profession))
            print(", ".join(domicile))
            print(", ".join(memberships))

            for source in node.get("@sources"):
                record = json.loads(source)
                print()
                print(" ", record.get("source"))
                print(json.dumps(record.get("data"), indent=4, ensure_ascii=False)[1:-1])

        self.storage.find_duplicates(type, callback)


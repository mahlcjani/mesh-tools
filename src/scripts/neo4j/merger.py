from collections.abc import Callable
from neo4j import Session, Transaction
from neo4j.exceptions import ResultNotSingleError
from typing import Any, Self
from meshtools.construct.engine \
    import Contains, LinkedObject, Object, ObjectId, ObjectKeys, PersistedObject, Storage


class Merger(Storage):

    """This is where knowledge is, callbacks should be dumb"""

    def __init__(self: Self, session: Session) -> None:
        self.session = session

    def match(self: Self, keys: ObjectId | ObjectKeys) -> ObjectId | None:
        try:
            return ObjectId(keys.type, self.session.execute_read(match_by_id, keys)) if isinstance(keys, ObjectId) \
                else ObjectId(keys.type, self.session.execute_read(match_by_keys, keys))
        except ResultNotSingleError:
            return None

    def create(self: Self, type: str, name: str, data: Any) -> ObjectId:
        return ObjectId(type, self.session.execute_write(create_node, type, name, data))

    def merge(self: Self, type: str, name: str, data: Any) -> ObjectId:
        return ObjectId(type, self.session.execute_write(merge_node, type, name, data))

    def merge_objects(self: Self, nodelist: list[ObjectId]) -> ObjectId:
        def run(tx: Transaction) -> str:
            return tx.run(
                """
                MATCH (n) WHERE elementId(n) IN $ids
                WITH collect(n) AS nodes
                CALL apoc.refactor.mergeNodes(nodes, {
                    properties: {
                        `@sources`: "combine",
                        domicile: "combine",
                        profession: "combine",
                        `.*`: "discard"
                    },
                    mergeRels: true,
                    singleElementAsArray: false
                })
                YIELD node
                RETURN elementId(node)
                """, ids=[id.id for id in nodelist]
            ).single().value()

        return ObjectId(nodelist[0].type, self.session.execute_write(run))

    def join(self: Self, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        return self.session.execute_write(join_nodes, whole, part)

    def find_duplicates(
        self: Self,
        type: str,
        callback: Callable[[str, int, list[LinkedObject]], None] | None = None
    ) -> list[tuple[str, int, list[LinkedObject]]] | None:
        duplicates = []

        def aggregator(name: str, count: int, objects: list[LinkedObject]):
            duplicates.append([name, count, objects])

        self.session.execute_read(find_duplicate_nodes, type, callback if callback else aggregator)

        return None if callback else duplicates


RELATIONS = [
    ("ChamberTerm", "term_of", "Chamber"),
    ("Elections", "to", "ChamberTerm"),
    ("Elections", "to", "Assembly"),
    ("Elections", "to", "Council"),
    ("Elections", "to", "Office"),
    ("ElectoralCommittee", "active_in", "Elections"),
    ("ParlimentaryGroup", "active_in", "ChamberTerm"),
    ("ParlimentaryGroup", "represents {distance: 1}", "Party"),
    ("Person", "candidate_for {distance: 10}", "Assembly"),
    ("Person", "elected_to {distance: 10}", "ChamberTerm"),
    ("Person", "candidate_in {distance: 10}", "Elections"),
    ("Person", "candidate_for {distance: 10}", "Council"),
    ("Person", "nominated_by {distance: 2}", "ElectoralCommittee"),
    ("Person", "candidate_for {distance: 10}", "Office"),
    ("Person", "member_of {distance: 1}", "ParlimentaryGroup"),
    ("Person", "member_of {distance: 1}", "Party"),
]

RELMAP = {}

for (part, rel, whole) in RELATIONS:
    m = RELMAP.get(part, dict())
    m[whole] = rel
    RELMAP[part] = m

LABELS = {
    "Person": ["Person"],

    "Party": ["Org", "Party"],
    "Alliance": ["Org", "Alliance"],

    "Chamber": ["Org", "Chamber"],
    "ChamberTerm": ["Org", "ChamberTerm"],
    "ParlimentaryGroup": ["Org", "ParlimentaryGroup"],

    "Elections": ["Event", "Elections"],
    "ElectoralCommittee": ["Org", "ElectoralCommittee"],
    "Assembly": ["Org", "Assembly"],
    "Council": ["Org", "Council"],
    "Office": ["Office"],
}


def labels(type: str) -> list[str]:
    return LABELS[type]


def match_by_id(tx: Transaction, id: str) -> str:
    # single(True) will raise exception if not exactly one result
    return tx.run(
        "MATCH (n) WHERE elementId(n) = $id RETURN elementId(n)",
        id=id
    ).single(True).value()


def match_by_keys(tx: Transaction, keys: ObjectKeys) -> str:
    if isinstance(keys, Contains):
        name, value = keys.get()

        return tx.run(
            f"MATCH (n:{keys.type}) WHERE $value IN n.{name} RETURN elementId(n)",
            value=value
        ).single(True).value()

    return tx.run(
        f"MATCH (n:{keys.type} {{$keys}}) RETURN elementId(n)",
        keys=keys.keys
    ).single(True).value()


def create_node(tx: Transaction, type: str, name: str, data: Any) -> str:
    return tx.run(
        f"""
        CREATE (n:{":".join(labels(type))} {{name: $name}})
            SET n += $properties
        RETURN elementId(n)
        """,
        name=name,
        # Make sure name is taken from name arg
        properties=data | {"name": name}
    ).single().value()


def merge_node(tx: Transaction, type: str, name: str, data: Any) -> str:
    return tx.run(
        f"""
        MERGE (n:{":".join(labels(type))} {{name: $name}})
            SET n += $properties
        RETURN elementId(n)
        """,
        name=name,
        # Make sure name is taken from name arg
        properties=data | {"name": name}
    ).single().value()


def join_nodes(tx: Transaction, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
    # TODO works only on ids!
    tx.run(
        f"""
        MATCH (whole:{whole.type}) WHERE elementId(whole) = $whole_id
        MATCH (part:{part.type}) WHERE elementId(part) = $part_id
        MERGE (part)-[r:{RELMAP[part.type][whole.type]}]->(whole)
        RETURN elementId(r)
        """, whole_id=whole.id, part_id=part.id
    ).consume()


def find_duplicate_nodes(
    tx: Transaction, type: str, callback: Callable[[str, int, list[LinkedObject]], None]
) -> None:

    result = tx.run(f"""
        MATCH (n:{type})
        WITH n.name AS name, count(n.name) AS count
        WHERE count > 1

        WITH name, count
        MATCH (n:{type} {{name: name}})
        OPTIONAL MATCH (n)-[:member_of]->(o)

        WITH *, {{
            id: elementId(n),
            element: n,
            links: collect(o)
        }} AS node

        RETURN name, count, collect(node) AS nodelist
        ORDER BY name
        """
    )

    for record in result:
        name = record.value("name")
        count = record.value("count")
        objects = [
            (
                (
                    ObjectId(type, node.get("id")),
                    node.get("element")
                ),
                node.get("links")
            )
            for node in record.value("nodelist")
        ]
        callback(name, count, objects)


""" MATCH (n:Person)
WITH n.name AS name, collect(n) AS nodelist, count(*) AS count
WHERE count > 1
CALL apoc.export.json.data(nodelist, [], null, {stream: true, writeNodeProperties: true})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN data


MATCH (n:Person)
  WHERE COUNT {
    MATCH (
      m:Person {
        name: n.name
      }
    )
  } > 1
RETURN elementId(n) AS id, n.name AS name, coalesce(n.birthYear, "") AS birthYear, n.domicile AS domicile, n.profession AS profession, n.createdFrom AS createdFrom
ORDER BY n.name


"""
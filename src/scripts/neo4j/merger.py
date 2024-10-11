from neo4j import Session, Transaction
from typing import Any, Self
from meshtools.construct.engine import Contains, ObjectId, ObjectKeys, Storage


class Merger(Storage):
    def __init__(self: Self, session: Session) -> None:
        self.session = session

    def init_namespace(self: Self, namespace: str, clean: bool = False) -> None:
        if namespace and clean:
            # Delete all nodes with attribute namespace
            self.session.execute_write(clean_namespace, namespace)

    def match(self: Self, namespace: str, keys: ObjectId | ObjectKeys) -> ObjectId:
        return self.session.execute_read(match_by_id, keys) if isinstance(keys, ObjectId) \
            else self.session.execute_read(match_by_keys, namespace, keys)

    def create(self: Self, namespace: str, type: str, name: str, data: Any) -> ObjectId:
        return self.session.execute_write(create_node, namespace, type, name, data)

    def merge(self: Self, namespace: str, type: str, name: str, data: Any) -> ObjectId:
        return self.session.execute_write(merge_node, namespace, type, name, data)

    def join(self: Self, namespace: str, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
        return self.session.execute_write(join_nodes, namespace, whole, part)


relmap = {
    "ChamberTerm": {
        "Chamber": "term_of"
    },
    "ChamberTermElections": {
        "ChamberTerm": "for"
    },
    "ParlimentaryGroup": {
        "ChamberTerm": "active_in",
        "Party": "represents {distance: 1}"
    },
    "Person": {
        "ChamberTerm": "elected_to {distance: 10}",
        "ElectoralCommittee": "candidate_from {distance: 2}",
        "ChamberTermElections": "candidate_in {distance: 10}",
        "ParlimentaryGroup": "member_of {distance: 1}",
        "Party": "member_of {distance: 1}",
    }
}


def labels(type: str) -> list[str]:
    return [type] if type == "Person" else ["Org", type]


def clean_namespace(tx: Transaction, namespace: str) -> None:
    return tx.run("MATCH (n {namespace: $namespace}) DETACH DELETE n", namespace=namespace).consume()


def match_by_id(tx: Transaction, id: str) -> str:
    return tx.run("MATCH (n) WHERE elementId(n) = $id RETURN elementId(n)", id=id).single()[0]


def match_by_keys(tx: Transaction, namespace: str, keys: ObjectKeys) -> str:
    if isinstance(keys, Contains):
        name, value = keys.get()

        result = tx.run(f"""
            MATCH (n:{keys.type})
            WHERE n.namespace = $namespace AND $value IN n.{name}
            RETURN elementId(n)
            """, namespace=namespace, value=value
        )

        records = result.fetch(2)
        if len(records) == 1:
            return records[0].value()

        # No object or keys do not allow to identify object unique,
        # return of none is rather not good
        return None

    return tx.run(f"""
        MATCH (n:{keys.type} {{$keys}})
        RETURN elementId(n)
        """, keys={"namespace": namespace} | keys.keys
    ).single()[0]


def create_node(tx: Transaction, namespace: str, type: str | list[str], name: str, data: Any) -> str:
    return tx.run(f"""
        CREATE (n:{":".join(labels(type))} {{namespace: $namespace, name: $name}})
            SET n = $properties
        RETURN elementId(n)
        """,
        name=name,
        namespace=namespace,
        properties=data | {"name": name, "namespace": namespace}
    ).single()[0]


def merge_node(tx: Transaction, namespace: str, type: str | list[str], name: str, data: Any) -> str:
    return tx.run(f"""
        MERGE (n:{":".join(labels(type))} {{namespace: $namespace, name: $name}})
            SET n += $properties
        RETURN elementId(n)
        """,
        name=name,
        namespace=namespace,
        properties=data | {"name": name, "namespace": namespace}
    ).single()[0]


def join_nodes(tx: Transaction, namespace: str, whole: ObjectId | ObjectKeys, part: ObjectId | ObjectKeys) -> None:
    print("join", part, relmap[part.type][whole.type], whole)

    # TODO works only on ids!
    tx.run(f"""
        MATCH (whole:{whole.type}) WHERE elementId(whole) = $whole_id
        MATCH (part:{part.type}) WHERE elementId(part) = $part_id
        MERGE (part)-[r:{relmap[part.type][whole.type]}]->(whole)
        RETURN elementId(r)
        """, whole_id=whole.id, part_id=part.id
    ).consume()


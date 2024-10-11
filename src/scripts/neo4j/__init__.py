import argparse
import json
import os

from neo4j import GraphDatabase
from typing import Any, Self

from meshtools.construct.engine import Contains, ObjectId, ObjectKeys, DataEngine
from .merger import Merger


def cypher_run() -> None:
    parser = argparse.ArgumentParser(description="Run cypher script")
    parser.add_argument(
        "-f", "--file",
        dest="file",
        action="store",
        help="Cypher script file"
    )
    parser.add_argument(
        "-a", "--uri",
        dest="uri",
        action="store",
        default="neo4j://localhost:7687",
        help="address and port to connect to, defaults to neo4j://localhost:7687"
    )
    parser.add_argument(
        "-u", "--username",
        dest="username",
        action="store",
        default="neo4j",
        help="username to connect as"
    )
    parser.add_argument(
        "-p", "--password",
        dest="password",
        action="store",
        help="password to connect with"
    )
    parser.add_argument(
        "-d", "--database",
        dest="database",
        action="store",
        help="database to connect to"
    )

    args = parser.parse_args()

    def is_valid(stmt: str) -> bool:
        for line in [line.strip().lower() for line in stmt.splitlines()]:
            if line and not line.startswith("//") \
                    and ("create" in line or "match" in line or "merge" in line):
                return True
        return False

    with GraphDatabase.driver(args.uri, auth=(args.username, args.password)) as driver:
        with driver.session(database=args.database) as session:
            with open(args.file, encoding="UTF8") as file:
                print("File:", args.file)
                for stmt in file.read().split(";"):
                    if is_valid(stmt):
                        print(session.run(stmt).consume().counters)


def import_term() -> None:
    parser = argparse.ArgumentParser(description="""
        Import files from directory
    """)

    # Import type
    parser.add_argument(
        dest="what",
        choices=["term", "parlimentary-elections", "eu-elections", "municipal-elections"],
        action="store",
        help="what to do"
    )
    # Import what=term

    # Import what=parliment-elections

    # import parliment-elections --chamber Sejm --term "Sejm X Kadencji" --dir

    # Shared arguments
    parser.add_argument(
        "-d", "--dir",
        dest="directory",
        action="store",
        help="Directory containing data to import"
    )
    parser.add_argument(
        "-c", "--chamber",
        dest="chamber_name",
        action="store",
        help="Chamber/house name"
    )
    parser.add_argument(
        "-t", "--term",
        dest="chamber_term",
        action="store",
        help="Chamber term name"
    )
    parser.add_argument(
        "-n", "--namespace",
        dest="namespace",
        action="store",
        help="namespace (branch) to import data to; if omitted data will be merged into trunk"
    )
    parser.add_argument(
        "-e", "--erase",
        dest="erase",
        action="store_true",
        help="should data in namespace (branch) to erased"
    )
    parser.add_argument(
        "-a", "--uri",
        dest="uri",
        action="store",
        default="neo4j://localhost:7687",
        help="address and port to connect to, defaults to neo4j://localhost:7687"
    )
    parser.add_argument(
        "-U", "--username",
        dest="username",
        action="store",
        default="neo4j",
        help="username to connect as"
    )
    parser.add_argument(
        "-P", "--password",
        dest="password",
        action="store",
        help="password to connect with"
    )
    parser.add_argument(
        "-D", "--database",
        dest="database",
        action="store",
        help="database to connect to"
    )

    args = parser.parse_args()

    with GraphDatabase.driver(args.uri, auth=(args.username, args.password)) as driver:
        with driver.session(database=args.database) as session:

            engine = DataEngine(Merger(session))
            if (args.namespace):
                engine = engine.use_namespace(args.namespace, args.erase)

            match args.what:
                case "term":
                    import_chamber_term(
                        engine,
                        args.directory,
                        chamber_name=args.chamber_name
                    )
                case "parlimentary-elections":
                    import_chamber_term_elections(
                        engine,
                        args.directory,
                        chamber_name=args.chamber_name,
                        chamber_term=args.chamber_term
                    )


def import_chamber_term(engine: DataEngine, directory: str, **kwargs: str) -> None:
    term_id: ObjectId = None
    chamber_name = kwargs.get("chamber_name")

    # Load term file
    with open(os.path.join(directory, "term.json"), "r", encoding="UTF8") as file:
        term = json.loads(file.read())
        term_id = engine.upsert("ChamberTerm", term.get("name"), term)
        print("  ->", term_id)
        if chamber_name:
            engine.join(engine.upsert("Chamber", chamber_name, {}), term_id)

    # Load parliment groups
    with open(os.path.join(directory, "clubs.json"), "r", encoding="UTF8") as file:
        for group in json.loads(file.read()):
            group_name = group.get("name")
            if group_name != "niez.":
                group_id = engine.insert("ParlimentaryGroup", group_name, group)
                print("  ->", group_id)
                engine.join(term_id, group_id)

    # Load people file
    with open(os.path.join(directory, "mp.json"), "r", encoding="UTF8") as file:
        for person in json.loads(file.read()):
            group_name = person.pop("parlimentaryGroup", "niez.")
            person_id = engine.upsert("Person", person.get("name"), person)
            print("  ->", person_id)
            engine.join(term_id, person_id)
            if group_name != "niez.":
                group_id = engine.upsert("ParlimentaryGroup", group_name, {})
                print("  ", "->", group_id)
                engine.join(group_id, person_id)


def import_chamber_term_elections(engine: DataEngine, directory: str, **kwargs: str) -> None:
    chamber_term = kwargs.get("chamber_term")
    # Load candidates  (temp)
    with open(os.path.join(directory, "candidates.json"), "r", encoding="UTF8") as file:
        for person in json.loads(file.read()):
            party = person.pop("party", None)
            electoral_committee: str = person.pop("electoralCommittee")

            person_id = engine.upsert("Person", person.get("name"), person)
            print("  ->", person_id)

            if party:
                party_id = engine.match(Contains("Party", "names", party))
                if not party_id:
                    party_id = engine.upsert("Party", party, {"names": [party]})

                engine.join(party_id, person_id)

            if electoral_committee:
                committee_id = engine.upsert(
                    "ElectoralCommittee", f"{electoral_committee} ({chamber_term})", {}
                )
                engine.join(committee_id, person_id)
                # TODO: link committee with party or alilance
                # ec_type, ec_name = parse_electoral_committee(electoral_committee)
                # if ec_type == "KKW":
                #
                # elif ec_type == "KW":


def parse_electoral_committee(name: str) -> tuple[str | None, str | None]:
    match name.casefold():
        case "koalicyjny komitet wyborczy ":
            return ("KKW", name[len("koalicyjny komitet wyborczy "):])
        case "komitet wyborczy wyborców ":
            return ("KWW", name[len("komitet wyborczy wyborców "):])
        case "komitet wyborczy ":
            return ("KW", name[len("komitet wyborczy "):])
        case _:
            return (None, None)


# MATCH p=shortestPath((m:Person {name: "Sławomir Jerzy Mentzen"})-[*1..5]-(b:Person {name: "Krzysztof Bosak"})) RETURN p
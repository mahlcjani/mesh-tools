import argparse
import json
import os
import shutil

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


def import_data() -> None:
    parser = argparse.ArgumentParser(description="""
        Import files from directory
    """)

    # Import type
    parser.add_argument(
        dest="what",
        choices=["term", "parlimentary-elections", "eu-elections", "local-elections"],
        action="store",
        help="what to do"
    )
    # Import what=term

    # Import what=parliment-elections

    # import parliment-elections --chamber Sejm --term "Sejm X Kadencji" --dir

    # Shared arguments
    parser.add_argument(
        "-p", "--path",
        dest="path",
        action="store",
        help="Directory or file containing data to import"
    )
    parser.add_argument(
        "-e", "--elections",
        dest="elections_name",
        action="store",
        help="Name of elections"
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

            match args.what:
                case "term":
                    import_chamber_term(
                        engine,
                        args.path,
                        chamber_name=args.chamber_name
                    )
                case "parlimentary-elections":
                    import_elections(engine, args.path, elections_name=args.elections_name)
                case "local-elections":
                    import_elections(engine, args.path, elections_name=args.elections_name)
                case "duplicates":
                    engine.report_duplicates("Person")



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


def import_elections(engine: DataEngine, path: str, **kwargs: str) -> None:

    elections_name = kwargs.get("elections_name")

    # Load candidates  (temp)
    with open(path, "r", encoding="UTF8") as file:

        # merge elections record
        elections_id = engine.upsert("Elections", elections_name)
        committee_lookup = dict[str, ObjectId]()

        def merge_electoral_committee(elections_id: str, electoral_committee):
            electoral_committee_id = committee_lookup.get(electoral_committee)
            if not electoral_committee_id:
                # electoral_committee_id = engine.join(elections_id, "ElectoralCommittee", electoral_committee)
                electoral_committee_id = engine.insert("ElectoralCommittee", electoral_committee)
                engine.join(elections_id, electoral_committee_id)
                committee_lookup[electoral_committee] = electoral_committee_id

            return electoral_committee_id

        # print("\033[?25l", end="")
        # print("\033[?25h", end="")
        progress = ProgressBar(len(json.load(file)), prefix=f"{elections_name} ({file.name[-30:]:.>32})")

        file.seek(0)
        for person in json.load(file):
            progress.move()

            parties = person.pop("@parties", [])
            electoral_committee = person.pop("@electoralCommittee", None)
            assembly = person.pop("@assembly", None)
            council = person.pop("@council", None)
            office = person.pop("@office", None)

            person_id = engine.insert("Person", person.get("name"), person)

            for party in parties:
                party_id = engine.match(Contains("Party", "names", party))
                if not party_id:
                    party_id = engine.upsert("Party", party, {"names": [party]})

                engine.join(party_id, person_id)

            if electoral_committee:
                electoral_committee_id = merge_electoral_committee(elections_id, electoral_committee)
                engine.join(electoral_committee_id, person_id)
                # TODO: link committee with party or aliance
                # ec_type, ec_name = parse_electoral_committee(electoral_committee)
                # if ec_type == "KKW":
                #
                # elif ec_type == "KW":

            if assembly:
                assembly_id = engine.upsert("Assembly", assembly, {})
                engine.join(assembly_id, person_id)
                # if elected:

            if council:
                council_id = engine.upsert("Council", council, {})
                engine.join(council_id, person_id)

            if office:
                office_id = engine.upsert("Office", office, {})
                engine.join(office_id, person_id)


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


class ProgressBar:
    def __init__(self: Self, total: int, prefix="Progress", suffix="Complete", fill="#") -> None:
        self.step = 0
        self.total = total
        self.prefix = prefix
        self.suffix = suffix
        self.fill = fill
        # To support console resize do it in draw() method
        self.bar_length = shutil.get_terminal_size((80, 20))[0] - len(f"{prefix} [] 100% {suffix}")
        self.draw()

    def draw(self: Self) -> None:
        percent = 100 * self.step // self.total
        filled_length = int(self.bar_length * self.step // self.total)
        bar = self.fill * filled_length + " " * (self.bar_length - filled_length)
        print(f"\r{self.prefix} [{bar}] {percent:3d}% {self.suffix}", end="\r")
        if self.step == self.total:
            print()

    def move(self: Self, steps: int = 1) -> None:
        self.step += steps
        self.draw()


def manage_data() -> None:
    parser = argparse.ArgumentParser(description="""
        Manage Mesh database
    """)

    # Import type
    parser.add_argument(
        dest="command",
        choices=["report-duplicates", "resolve-duplicates"],
        action="store",
        help="what to do"
    )

    # Shared arguments
    parser.add_argument(
        "-p", "--path",
        dest="path",
        action="store",
        help="Directory or file containing data to import"
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

            match args.command:
                case "report-duplicates":
                    engine.report_duplicates("Person")
                case "resolve-duplicates":
                    resolve_duplicates(engine, args.path)


def resolve_duplicates(engine: DataEngine, path: str, **kwargs: str) -> None:
    def merge_nodes(nodes: list[str]) -> None:
        if nodes:
            engine.merge([ObjectId("Person", id) for id in nodes])
            nodes.clear()

    with open(path, "r", encoding="UTF8") as file:
        nodes = []
        while True:
            line = file.readline()
            if not line:
                break

            match line[0:2]:
                case "--":
                    merge_nodes(nodes)
                case "& ":
                    merge_nodes(nodes)
                    nodes.append(line[2:].strip())
                case "^ ":
                    nodes.append(line[2:].strip())

        merge_nodes(nodes)


#match (p:Person {name: "Jarosław Aleksander Kaczyński"})
#with collect(p) as nodes
#CALL apoc.refactor.mergeNodes(nodes,{
#  properties:"combine",
#  mergeRels:true
#})
#YIELD node
#RETURN node;

# match (p:Person) return p.name, collect(elementId(p))


#MATCH (n:Person) WHERE size(n.names) > 1
#WITH n.name AS name, collect(n) AS nodes, count(*) AS count
#WHERE count > 1
#CALL apoc.refactor.mergeNodes(nodes,{
#  properties:"combine",
#  mergeRels:true
#})
#YIELD node
#RETURN node

#RETURN name, nodelist, count


# MATCH p=shortestPath((m:Person {name: "Sławomir Jerzy Mentzen"})-[*1..5]-(b:Person {name: "Krzysztof Bosak"})) RETURN p

"""
MATCH (n:Person)
RETURN DISTINCT n.name AS name, n.domicile AS domicile, COLLECT {
  MATCH (m:Person)
    WHERE m.name = n.name
    AND m.domicile = n.domicile
  RETURN elementId(m)
} AS ids
ORDER BY name

MATCH (n:Person)
RETURN DISTINCT n.name AS name, n.profession AS profession, COLLECT {
  MATCH (m:Person)
    WHERE m.name = n.name
    AND m.profession = n.profession
  RETURN elementId(m)
} AS ids
ORDER BY name

MATCH (n:Person)
RETURN DISTINCT n.name AS name, n.domicile AS domicile, n.profession AS profession, COLLECT {
  MATCH (m:Person)
    WHERE m.name = n.name
    AND m.domicile = n.domicile
    AND m.profession = n.profession
  RETURN elementId(m)
} AS ids
ORDER BY name




MATCH (n:Person) WHERE COUNT { MATCH (m:Person {name: n.name}) } > 1
RETURN DISTINCT n.name AS name, n.domicile AS domicile, n.profession AS profession, COLLECT {
  MATCH (m:Person)
    WHERE m.name = n.name
    AND m.domicile = n.domicile
    AND m.profession = n.profession
  RETURN elementId(m)
} AS ids
ORDER BY name


MATCH (n:Person)
  WHERE COUNT {
    MATCH (
      m:Person {
        name: n.name,
        domicile: n.domicile,
        profession: n.profession
      }
    )
  } > 1
RETURN DISTINCT n.name AS name, n.domicile AS domicile, n.profession AS profession, COLLECT {
    MATCH (
      m:Person {
        name: n.name,
        domicile: n.domicile,
        profession: n.profession
      }
    )
    RETURN elementId(m)
} AS ids
ORDER BY name



MATCH (n:Person)
  WHERE COUNT {
    MATCH (
      m:Person {
        name: n.name,
        domicile: n.domicile,
        profession: n.profession
      }
    )
  } > 1

WITH COLLECT {
  MATCH (
    m:Person {
      name: n.name,
      domicile: n.domicile,
      profession: n.profession
    }
  )
  RETURN m
} AS nodes

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

RETURN node.name

"""
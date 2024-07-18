import argparse
import csv
import json
import sys
import tomllib

from ..mapping.names import CompositeMapper, BirthdateMapper, NameMapper

def csv2json():
    parser = argparse.ArgumentParser(description="Convert CSV to JSON")
    parser.add_argument(
        "-i", "--csv-file",
        dest="infile",
        action="store",
        help="CSV file to read from (stdin if not specified)"
    )
    parser.add_argument(
        "--csv-delimiter",
        dest="delimiter",
        action="store",
        default=",",
        help="CSV field delimiter"
    )
    parser.add_argument(
        "--csv-encoding",
        dest="encoding",
        action="store",
        default="utf-8-sig",
        help="CSV file encoding"
    )
    parser.add_argument(
        "-o", "--json-file",
        dest="outfile",
        action="store",
        help="path to write JSON (stdout if not specified)"
    )

    args = parser.parse_args()

    csv_file = sys.stdin
    json_file = sys.stdout

    try:
        if (args.infile):
            csv_file = open(args.infile, encoding=args.encoding)

        if (args.outfile):
            json_file = open(args.outfile, encoding="utf-8", mode="w")

        csvReader = csv.DictReader(csv_file, dialect="excel", delimiter=args.delimiter)

        jsonArray = []

        for row in csvReader:
            jsonArray.append(row)

        json_file.write(json.dumps(jsonArray, indent=2, ensure_ascii=False))

    finally:
        if csv_file is not sys.stdin:
            csv_file.close()

        if json_file is not sys.stdout:
            json_file.close()

def normjson():
    parser = argparse.ArgumentParser(description="Convert input JSON to understood format")
    parser.add_argument(
        "-i", "--input-file",
        dest="input_file",
        action="store",
        help="JSON file to read from (stdin if not specified)"
    )
    parser.add_argument(
        "-o", "--output-file",
        dest="output_file",
        action="store",
        help="path to write normalized JSON (stdout if not specified)"
    )
    parser.add_argument(
        "-c", "--config-file",
        dest="config_file",
        action="store",
        help="path to config file with instructions"
    )
    parser.add_argument(
        "-s", "--retain-input",
        dest="retain_input",
        action="store_true",
        help="should input be retained"
    )
    parser.add_argument(
        "-n", "--retain-input-node",
        dest="input_node",
        action="store",
        help="node name under which to retain input"
    )

    args = parser.parse_args()

    # default config
    config = {}

    # merge config file into config
    if args.config_file:
        with open(args.config_file, mode="rb") as config_file:
            config.update(tomllib.load(config_file))

    # TODO: merge command line arguments into config

    input_file = sys.stdin
    output_file = sys.stdout

    try:
        input_file = open(args.input_file, encoding="utf-8") if args.input_file else sys.stdin

        # Load input
        data = json.loads(input_file.read())

        # Create mapper
        mapper = CompositeMapper([
            NameMapper(**config["keys"]),
            BirthdateMapper(**config["keys"])
        ], args.retain_input, args.input_node)

        output = mapper.map(data) if isinstance(data, dict) else list(map(lambda e: mapper.map(e), data))

        try:
            output_file = open(args.output_file, encoding="utf-8", mode="w") if args.output_file else sys.stdout
            output_file.write(json.dumps(output, indent=2, ensure_ascii=False))
            output_file.write("\n")
        finally:
            if output_file is not sys.stdout:
                output_file.close()

    finally:
        if input_file is not sys.stdin:
            input_file.close()

import argparse
import csv
import json
import sys
import tomllib

from typing import Any

from ..mapping.basic import IntProperty
from ..mapping.dates import DateFilter
from ..mapping.mapper import PropertiesFilter, FilterChain
from ..mapping.names import \
    FullnameBuilder, \
    FullnameFilter, \
    NamesFilter, \
    SurnameFilter


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
        if args.infile:
            csv_file = open(args.infile, encoding=args.encoding)

        if args.outfile:
            json_file = open(args.outfile, encoding="utf-8", mode="w")

        csv_reader = csv.DictReader(csv_file, dialect="excel", delimiter=args.delimiter)

        json_array = []

        for row in csv_reader:
            json_array.append(row)

        json_file.write(json.dumps(json_array, indent=2, ensure_ascii=False))

    finally:
        if csv_file is not sys.stdin:
            csv_file.close()

        if json_file is not sys.stdout:
            json_file.close()


def filterjson():
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
        "-f", "--filter",
        dest="filters",
        default=[],
        action="append",
        help="add filter and it's config in form filtername=property,options"
    )
    parser.add_argument(
        "-s", "--retain-input",
        dest="retain_input",
        action="store_true",
        help="should input be retained"
    )

    args = parser.parse_args()

    # default config
    config = {}

    # merge config file into config
    if args.config_file:
        with open(args.config_file, mode="rb") as config_file:
            config.update(tomllib.load(config_file))

    # Create filer
    filter = FilterChain(
        filters_from_cmdline(args.filters) +
        filters_from_config(config.get("filter", dict()))
    )

    input_file = sys.stdin
    output_file = sys.stdout

    try:
        input_file = open(args.input_file, encoding="utf-8") \
            if args.input_file else sys.stdin

        # Load input
        data = json.loads(input_file.read())

        output = filter.filter(data) if isinstance(data, dict) \
            else [filter.filter(e) for e in data]

        try:
            output_file = open(args.output_file, encoding="utf-8", mode="w") \
                if args.output_file else sys.stdout

            output_file.write(json.dumps(output, indent=2, ensure_ascii=False))
            output_file.write("\n")
        finally:
            if output_file is not sys.stdout:
                output_file.close()

    finally:
        if input_file is not sys.stdin:
            input_file.close()


def filters_from_config(config: dict[str, Any]) -> list[PropertiesFilter]:
    mappers = []

    fullnameMapperConfigs = config.get("fullname", dict())
    for name, c in fullnameMapperConfigs.items():
        mappers.append(FullnameFilter(name, **c))

    dateMapperConfigs = config.get("date", dict())
    for name, c in dateMapperConfigs.items():
        mappers.append(DateFilter(name, **c))

    return mappers


def filters_from_cmdline(filters: list[str]) -> list[PropertiesFilter]:
    mappers = []

    for filter_config in filters:
        filter, property, string_opts, *_ = filter_config.split(":")
        opts = tomllib.loads(f"opts={{{string_opts}}}")["opts"]
        mappers.append(globals()[filter](property, **opts))

    return mappers

"""Helper functions for handling JSON files

This module is a wrapper around the json module to simplify JSON handling in MyTardis."""

import datetime
import json
from pathlib import Path


def read_json(json_file: Path) -> dict:
    """Reads and parses a JSON formatted file

    Args:
        json_file: The file path to the JSON formatted file

    Returns:
        A python dictionary containing the parsed file
    """

    try:
        with open(json_file, "r", encoding="utf8") as in_file:
            json_dict = json.load(in_file)
    except Exception as err:
        raise err
    return json_dict


def write_json(json_dict: dict, json_file: Path) -> None:
    """Writes a JSON formatted file from a python dictionary

    Args:
        json_dict: The python dictionary to be serialised
        json_file: The file path to the JSON formatted file

    Returns:
        None
    """

    try:
        with open(json_file, "w", encoding="utf8") as out_file:
            json.dump(json_dict, out_file, indent=4)
    except Exception as err:
        raise err


def dict_to_json(dictionary: dict) -> str:
    """Adds datetime object handling to the JSON serialiser

    Serialise a dictionary to JSON, correctly handling datetime.datetime
    objects (to ISO 8601 dates, as strings).

    Args:
        dictionary: Dictionary to serialise

    Returns:
        JSON string

    Raises:
        TypeError: if the data passed is not a dictionary
    """

    if not isinstance(dictionary, dict):
        raise TypeError("Must be a dictionary")

    def date_handler(obj):
        return (
            obj.isoformat()
            if isinstance(obj, (datetime.date, datetime.datetime))
            else None
        )

    try:
        json_string = json.dumps(dictionary, default=date_handler)
    except Exception as err:
        raise err
    return json_string

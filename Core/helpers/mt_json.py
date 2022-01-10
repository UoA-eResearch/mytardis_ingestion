# mt_json.py
#
# Helper functions for reading/writing JSONs for MyTardis ingestion
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 29 May 2020
#

from pathlib import Path
import json
import datetime


def readJSON(json_file):
    '''
    Function to parse a JSON formatted file
    #
    ======================
    Inputs
    ======================
    json_file: The file path to the JSON formatted file
    #
    ======================
    Returns
    ======================
    json_dict: A python dictionary containing the parsed file
    '''

    try:
        with open(json_file, 'r') as in_file:
            json_dict = json.load(in_file)
    except Exception as err:
        raise err
    return json_dict


def writeJSON(json_dict, json_file):
    '''
    Function to write a JSON formatted file from a python dictionary
    #
    ======================
    Inputs
    ======================
    json_dict: The python dictionary to be serialised
    json_file: The file path to the JSON formatted file
    #
    ======================
    Returns
    ======================
    True if the file is created successfully
    '''

    try:
        with open(json_file, 'w') as out_file:
            json.dump(json_dict, out_file, indent=4)
        return True
    except Exception as err:
        raise err


def dict_to_json(dictionary):
    '''
    Serialize a dictionary to JSON, correctly handling datetime.datetime
    objects (to ISO 8601 dates, as strings).
    #
    ======================
    Inputs
    ======================
    dictionary: Dictionary to serialise
    #
    ======================
    Returns
    ======================
    JSON string
    '''

    if not isinstance(dictionary, dict):
        raise TypeError("Must be a dictionary")

    def date_handler(obj): return (
        obj.isoformat()
        if isinstance(obj, datetime.date)
        or isinstance(obj, datetime.datetime)
        else None
    )
    try:
        json_string = json.dumps(dictionary, default=date_handler)
    except Exception as err:
        raise err
    return json_string

# Common helper functions

import logging

def readJSON(json_file, logger=None):
    '''Function to parse a JSON formatted file

    Inputs:
    =================================
    json_file: The file path to the JSON formatted file
    logger (optional): A logging object for information

    Returns:
    =================================
    json_dict: A python dictionary containing the parsed file
    '''
    import json
    try:
        with open(json_file, 'r') as in_file:
            json_dict = json.load(in_file)
    except FileNotFoundError as fnferr:
        if logger:
            logger.exception(fnferr)
        raise
    except Exception as err:
        if logger:
            logger.error(f'JSON file, {json_file}, unable to be read into dictionary')
            logger.exception(err)
        raise
    else:
        if logger:
            logger.debug(f'JSON file, {json_file}, sucessfully loaded')
        return json_dict

def writeJSON(json_dict, json_file, logger=None):
    '''Function to write a JSON formatted file from a python dictionary

    Inputs:
    =================================
    json_dict: The python dictionary to be serialised
    json_file: The file path to the JSON formatted file
    logger (optional): A logging object for information

    Returns:
    =================================
    True if the file is created successfully
    '''
    import json
    try:
        with open(json_file, 'w') as out_file:
            json.dump(json_dict, out_file)
        if logger:
            logger.debug(f'JSON {json_file} successfully written')
        return True
    except Exception as err:
        if logger:
            logger.error(f'JSON file, {json_file}, unable to be written')
            logger.exception(err)
        raise

def lowercase(obj):
    """ Make dictionary lowercase """
    if isinstance(obj, dict):
        return {k.lower():lowercase(v) for k, v in obj.items()}
    elif isinstance(obj, (list, set, tuple)):
        t = type(obj)
        return t(lowercase(o) for o in obj)
    elif isinstance(obj, str):
        return obj.lower()
    else:
        return obj

def check_dictionary(self,
                     dictionary,
                     required_keys):
    '''Carry out basic sanity tests on a dictionary

    Inputs:
    =================================
    dictionary: The dictionary to check
    required_keys: A list of required keys for the dictionary

    Returns:
    =================================
    True and an empty list if all the keys are found
    False and a list of missing keys if some are missing
    '''
    lost_keys = []
    for key in required_keys:
        if key not in dictionary.keys():
            lost_keys.append(key)
    if lost_keys == []:
        return (True, [])
    else:
        return (False, lost_keys)

def dict_to_json(dictionary):
    """
    Serialize a dictionary to JSON, correctly handling datetime.datetime
    objects (to ISO 8601 dates, as strings).
    
    Input:
    =================================
    dictionary: Dictionary to serialise
        
    Returns:
    =================================
    JSON string
    """
    if not isinstance(dictionary, dict):
        raise TypeError("Must be a dictionary")
        
    def date_handler(obj): return (
        obj.isoformat(' ')
        if isinstance(obj, datetime.date)
        or isinstance(obj, datetime.datetime)
        else None
    )
    return json.dumps(dictionary, default=date_handler)

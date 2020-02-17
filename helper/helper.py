# Common helper functions

import os
import hashlib
import subprocess
import logging
from . import constants as CONST
from pathlib import Path

logger = logging.getLogger(__name__)

def readJSON(json_file):
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

def writeJSON(json_dict, json_file):
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

def check_dictionary(dictionary,
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
    import json
    import datetime
    if not isinstance(dictionary, dict):
        raise TypeError("Must be a dictionary")

    def date_handler(obj): return (
        obj.isoformat()
        if isinstance(obj, datetime.date)
        or isinstance(obj, datetime.datetime)
        else None
    )
    return json.dumps(dictionary, default=date_handler)

def calculate_etag(file_path,
                   blocksize):
    md5s = []
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(blocksize)
            if not chunk:
                break
            md5s.append(hashlib.md5(chunk))
    if len(md5s) > 1:
        digests = b"".join(m.digest() for m in md5s)
        new_md5 = hashlib.md5(digests)
        etag = new_md5.hexdigest() + '-' + str(len(md5s))
    elif len(md5s) == 1:
        etag = md5s[0].hexdigest()
    else:
        etag = '""'
    return etag

def calculate_md5sum(file_path,
                     blocksize=None,
                     subprocess_size_threshold=10*CONST.MB,
                     md5sum_executable='/usr/bin/md5sum'):
    """
    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.
    If the file size is greater than subprocess_size_threshold and the
    md5sum tool exists, spawn a subprocess and use 'md5sum', otherwise
    use the native Python md5 method (~ 3x slower).
    :type file_path: Path
    :type blocksize: int
    :param subprocess_size_threshold: Use the md5sum tool via a subprocess
                                        for files larger than this. Otherwise
                                        use Python native method.
    :type subprocess_size_threshold: int
    :return: The hex encoded MD5 checksum.
    :rtype: str
    """
    if os.path.getsize(file_path) > subprocess_size_threshold and \
       os.path.exists(md5sum_executable) and \
       os.access(md5sum_executable, os.X_OK):
        return md5_subprocess(file_path,
                              md5sum_executable=md5sum_executable)
    else:
        return md5_python(file_path,
                          blocksize=blocksize)
    
def md5_python(file_path,
               blocksize=None):
    """
    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.
    :type file_path: Path
    :type blocksize: int
    :return: The hex encoded MD5 checksum.
    :rtype: str
    """
    if not blocksize:
        blocksize = 128

    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(blocksize)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()

def md5_subprocess(file_path,
                   md5sum_executable='/usr/bin/md5sum'):
    """
    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.
    :type file_path: Path
    :return: The hex encoded MD5 checksum.
    :rtype: str
    """
    out = subprocess.check_output([md5sum_executable, file_path])
    checksum = out.split()[0]
    if len(checksum) == 32:
        return checksum
    else:
        raise ValueError('md5sum failed: %s', out)

def build_checksum_digest(checksum_digest,
                          root_dir,
                          file_path,
                          s3=True,
                          s3_blocksize=1*CONST.GB,
                          md5_blocksize=None,
                          subprocess_size_threshold=10*CONST.MB,
                          md5sum_executable='/usr/bin/md5sum'):
    """
    Builds a tuple of md5, etag checksums for a given
    data file and appends it to the checksum digest file for use
    by the ingestion classes.file_path
    
    =================================
    Inputs:
    =================================
    checksum_digest: a Path object to the file to append to
    file_path: a Path object to the file to build the checksums force
    s3: a boolean flag to indicate whether or not to calculate the ETag

    =================================
    Returns:
    =================================
    True: if checksums calculated and appended successfully
    False: otherwise
    """
    abs_file_path = root_dir / file_path
    try:
        md5 = calculate_md5sum(abs_file_path,
                               blocksize,
                               subprocess_size_threshold,
                               md5sum_executable)
    except FileNotFoundError as error:
        logger.error(error.message)
        return False
    except Exception as error:
        logger.error(error.message)
        raise
    if md5 == '':
        return False
    writestring = f'{file_path},{md5}'
    if s3:
        etag = calculate_etag(abs_file_path,
                              s3_blocksize)
        writestring += f',{etag}'
    writestring += f'\n'
    try:
        with open(checksum_digest, 'a') as filename:
            filename.write(writestring)
    except Exception as error:
        logger.error(error.message)
        return False
    return True
    
def read_checksum_digest(checksum_digest):
    ret_dict = {}
    try:
        with open(checksum_digest, 'r') as filename:
            for line in filename:
                data = line.split(',')
                if len(data) < 2:
                    logger.error(f'Malformed checksum digest file {checksum_digest}')
                    return False
                key = data.pop(0)
                ret_dict[key] = data
    except Exception as error:
        logger.error(error.message)
        raise
    return ret_dict

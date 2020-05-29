# MyTardisFoundry class
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated 29 May 2020

from ..exceptions import SanityCheckError, UnableToFindUniqueError
from rest import MyTardisREST
from helper import RAiDFactory
import json
from abc import ABC, abstractmethod


class MyTardisFoundry(ABC):

    ''' Define a generic class with functions to get data from
    MyTardis via the MyTardisREST class and to sanity check
    input dictionaries to make sure they contain at least the 
    bare minimum data required to create an object in MyTardis.'''

    def __init__(self,
                 local_config_filepath):
        ''' Required_keys are the dictionary items that must be present
        in order for an object of the class foundry that this is the
        parent of.'''

        self.rest = MyTardisREST(local_config_file_path)

    def sanity_check(input_dict,
                     required_keys):
        if not (required_keys - input_dict.keys()):
            return True
        else:
            missing_keys = list(required_keys - input_dict.keys())
            raise SanityCheckError(input_dict,
                                   missing_keys)

    def get_uri(self,
                action,
                search_target,
                search_string):
        query_params = {search_target: search_string}
        try:
            response = rest.get_request(action,
                                        query_params)
        except Exception as error:
            raise
        else:
            response_dict = json.loads(response.text)
            if response_dict = {} or response_dict['objects'] == []:
                return None
            elif len(response_dict['objects']) > 1:
                raise UnableToFindUniqueError(action,
                                              query_params,
                                              response_dict)
            else:
                obj = response_dict['objects'][0]
                uri = obj['resource_uri']
                return uri

    @abstractmethod
    def smelt(self):
        pass

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
    logger.debug('Building Checksum Digest\n============\n')
    abs_file_path = root_dir / file_path
    logger.debug(abs_file_path)
    checksum_dict = {}
    if not os.path.isfile(checksum_digest):
        checksum_dict = {}
    else:
        checksum_dict = readJSON(checksum_digest)
    logger.debug(checksum_dict)
    if not file_path in checksum_dict.keys():
        checksum_dict[file_path.as_posix()] = {}
    checksum_dict[file_path.as_posix()]['md5sum'] = calculate_md5sum(
        abs_file_path)
    if s3:
        checksum_dict[file_path.as_posix()]['etag'] = calculate_etag(abs_file_path,
                                                                     s3_blocksize)
    writeJSON(checksum_dict, checksum_digest)
    logger.debug(checksum_dict)
    return checksum_dict

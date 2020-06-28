# MyTardisFoundry class
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated 04 Jun 2020

from ..helpers import SanityCheckError
from ..helpers import readJSON, writeJSON
from ..helpers import calculate_md5sum, calculate_etag
import json


class MyTardisFoundry():

    ''' Define a generic class with functions to get data from
    MyTardis via the MyTardisREST class and to sanity check
    input dictionaries to make sure they contain at least the 
    bare minimum data required to create an object in MyTardis.'''

    def sanity_check(input_dict,
                     required_keys):
        if not (required_keys - input_dict.keys()):
            return True
        else:
            missing_keys = list(required_keys - input_dict.keys())
            raise SanityCheckError(input_dict,
                                   missing_keys)

        def smelt(self,
                  raw_dict,
                  model_keys):
            '''
            Takes a dictionary that is a mix of model and parameters
            and splits it into the model key/values and the parameter
            key/values for ingestion.
            #
            ======================
            Inputs
            ======================
            raw_dict:dict - combined dictionary
            model_keys:tuple - tuple of hardcoded model keys
            #
            ======================
            Returns
            ======================
            model: dict - dictionary of model key/value pairs
            params: dict - dictionary of parameter key/value pairs
            '''

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
        #
        =================================
        Inputs:
        =================================
        checksum_digest: a Path object to the file to append to
        file_path: a Path object to the file to build the checksums force
        s3: a boolean flag to indicate whether or not to calculate the ETag
        #
        =================================
        Returns:
        =================================
        True: if checksums calculated and appended successfully
        False: otherwise
        """
        abs_file_path = root_dir / file_path
        checksum_dict = {}
        if not os.path.isfile(checksum_digest):
            checksum_dict = {}
        else:
            checksum_dict = readJSON(checksum_digest)
        if not file_path in checksum_dict.keys():
            checksum_dict[file_path.as_posix()] = {}
        checksum_dict[file_path.as_posix()]['md5sum'] = calculate_md5sum(
            abs_file_path)
        if s3:
            checksum_dict[file_path.as_posix()]['etag'] = calculate_etag(abs_file_path,
                                                                         s3_blocksize)
        writeJSON(checksum_dict, checksum_digest)
        return checksum_dict

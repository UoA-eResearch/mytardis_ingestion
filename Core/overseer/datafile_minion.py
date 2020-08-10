# datafile_minion.py
#
# Minion class for the Datafile model
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 23 Jul 2020
#

from .minion import MyTardisMinion
from ..helpers import sanity_check
import json


class DatafileMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate datafile
    objects.
    '''

    def __init__(self,
                 local_config_file_path):
        super().__init__(local_config_file_path)

    def validate_dictionary(self,
                            input_dict):
        required_keys = ['dataset_id',
                         'filename',
                         'md5sum',
                         'storage_box',
                         'remote_path',
                         'local_path',
                         'schema',
                         'full_path']
        try:
            valid = sanity_check(input_dict,
                                 required_keys)
        except Exception as error:
            raise error
        return valid

    def check_file_exists(self,
                          input_dict):
        '''
        Search on the Dataset RAID and filename to get any files
        registered in MyTardis. If they exist compare MD5s and
        if they are the same return True, else False
        '''
        search_params = {'dataset__dataset_id': input_dict['dataset_id'],
                         'filename': input_dict['filename']}
        try:
            response = self.get_request('dataset_file',
                                        search_params)
        except Exception as error:
            raise error
        else:
            response_dict = json.loads(response.text)
            if response_dict == {} or response_dict['objects'] == []:
                return False
            objs = response_dict['objects']
            for obj in objs:
                if input_dict == obj['md5sum']:
                    return True
            return False

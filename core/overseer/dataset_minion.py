# dataset_minion.py
#
# Minion class for the Dataset model
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 05 Aug 2020
#

from .minion import MyTardisMinion
from ..helpers import sanity_check


class DatasetMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate dataset
    objects.
    '''

    def __init__(self,
                 local_config_file_path):
        super().__init__(local_config_file_path)

    def get_from_raid(self,
                      dataset_id):
        try:
            uri, obj = self.get_uri('dataset',
                                    'dataset_id',
                                    dataset_id)
        except Exception as error:
            raise error
        return (uri, obj)

    def validate_dictionary(self,
                            input_dict):
        required_keys = ['description',
                         'dataset_id',
                         'experiments',
                         'schema',
                         'instrument_id']
        try:
            valid = sanity_check(input_dict,
                                 required_keys)
        except Exception as error:
            raise error
        return valid

    def validate_schema(self,
                        input_dict):
        try:
            valid = sanity_check(input_dict,
                                 ['schema'])
        except Exception as error:
            raise error
        return valid

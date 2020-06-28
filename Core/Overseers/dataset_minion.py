# dataset_minion.py
#
# Minion class for the Dataset model
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 25 Jun 2020
#

from minion import MyTardisMinion
from ..helpers import sanity_check


class DatasetMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate dataset
    objects.
    '''

    def __init__(self,
                 global_config_filepath,
                 local_config__filepath):
        super().__init__(global_config_filepath,
                         local_config__filepath)

    def get_from_raid(self,
                      dataset_id):
        try:
            uri, _ = self.get_uri('dataset',
                                  'dataset_id',
                                  dataset_id)
        except Exception as error:
            raise error
        return uri

    def validate_dictionary(self,
                            input_dict):
        required_keys = ['description',
                         'dataset_id',
                         'experiments', ]
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

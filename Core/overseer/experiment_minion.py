# experiment_minion.py
#
# Minion class for the Experiment model
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 08 Jul 2020
#

from minion import MyTardisMinion
from ..helpers import sanity_check


class ExperimentMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate experiment
    objects.
    '''

    def __init__(self,
                 global_config_filepath,
                 local_config_filepath):
        super().__init__(global_config_filepath,
                         local_config_filepath)

    def get_from_raid(self,
                      raid):
        try:
            uri, _ = self.get_uri('experiment',
                                  'raid',
                                  raid)
        except Exception as error:
            raise error
        return uri

    def validate_dictionary(self,
                            input_dict):
        required_keys = ['title',
                         'raid',
                         'description',
                         'project',
                         'schema']
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

# project_minion.py
#
# Minon class for the Project model
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 13 Jul 2020
#

from minion import MyTardisMinion
from ..helpers import sanity_check


class ProjectMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate project
    objects.
    '''

    def __init__(self,
                 local_config_file_path):
        super().__init__(local_config_file_path)

    def get_from_raid(self,
                      raid):
        try:
            uri, obj = self.get_uri('project',
                                    'raid',
                                    raid)
        except Exception as error:
            raise error
        return (uri, obj)

    def validate_dictionary(self,
                            input_dict):
        required_keys = ['name',
                         'raid',
                         'description',
                         'lead_researcher',
                         'institution',
                         'schema']
        try:
            valid = sanity_check(input_dict,
                                 required_keys)
        except Exception as error:
            raise error
        return valid

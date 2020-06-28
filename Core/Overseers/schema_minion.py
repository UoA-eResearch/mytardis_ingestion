# schema_minion.py
#
# Minion class for generic schema
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 11 Jun 2020
#

from minion import MyTardisMinion
from ..helpers import sanity_check


class SchemaMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate schema
    objects.
    '''

    def __init__(self,
                 global_config_filepath,
                 local_config__filepath):
        super().__init__(global_config_filepath,
                         local_config__filepath)

    def get_from_namespace(self,
                           namespace):
        try:
            uri, response = self.get_uri('schema',
                                         'namespace',
                                         raid)
        except Exception as error:
            raise error
        return uri

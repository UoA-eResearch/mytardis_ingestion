# schema_minion.py
#
# Minion class for generic schema
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 23 Jul 2020
#

from .minion import MyTardisMinion
from ..helpers import sanity_check


class SchemaMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate schema
    objects.
    '''

    def __init__(self,
                 local_config__file_path):
        super().__init__(local_config__file_path)

    def get_from_namespace(self,
                           namespace):
        try:
            uri, obj = self.get_uri('schema',
                                    'namespace',
                                    namespace)
        except Exception as error:
            raise error
        return (uri, obj)

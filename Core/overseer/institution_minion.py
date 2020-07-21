# institution_minion.py
#
# Minion class for institutions
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 21 Jul 2020
#

from minion import MyTardisMinion
from ..helpers import sanity_check


class InstitutionMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate institution
    objects.
    '''

    def __init__(self,
                 global_config_filepath,
                 local_config__filepath):
        super().__init__(global_config_filepath,
                         local_config__filepath)

    def get_from_name(self,
                      name):
        uri = None
        obj = None
        try:
            uri, obj = self.get_uri('institution',
                                    'name',
                                    name)
        except Exception as error:
            raise error
        return (uri, obj)

    def get_from_ror(self,
                     ror):
        uri = None
        obj = None
        try:
            uri, obj = self.get_uri('institution',
                                    'ror',
                                    ror)
        except Exception as error:
            raise error
        return (uri, obj)

# institution_minion.py
#
# Minion class for institutions
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 23 Jul 2020
#

from .minion import MyTardisMinion
from ..helpers import sanity_check


class InstitutionMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate institution
    objects.
    '''

    def __init__(self,
                 local_config__file_path):
        super().__init__(local_config__file_path)

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

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


class InstrumentMinion(MyTardisMinion):
    '''
    Overseer Minion class to inspect and validate schema
    objects.
    '''

    def __init__(self,
                 local_config__file_path):
        super().__init__(local_config__file_path)

    def get_from_instrument_id(self,
                               instrument_id):
        uri = None
        obj = None
        try:
            uri, obj = self.get_uri('instrument',
                                    'instrument_id',
                                    instrument_id)
        except Exception as error:
            raise error
        return (uri, obj)

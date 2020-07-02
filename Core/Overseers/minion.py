# minion.py
#
# Class definition for the MyTardis minion class.
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 02 Jul 2020
#

from .. import MyTardisRESTFactory
from ..helpers import RAiDFactory
from ..helpers import UnableToFindUniqueError
import json


class MyTardisMinion(MyTardisRESTFactory):
    '''
    Overseer Minion classes inspect the MyTardis database to ensure
    that the forge is not creating existing objects and validates
    that the heirarchical structures needed are in place before
    a new object is created.
    Minions report back to the Overseer.
    '''

    def __init__(self,
                 global_config_filepath,
                 local_config_filepath):
        self.raid_factory = RAiDFactory(global_config_filepath)
        super().__init__(local_config__filepath)

    def resource_uri_to_id(self, uri):
        """
        Takes resource URI like: http://example.org/api/v1/experiment/998
        and returns just the id value (998).
        #
        ======================
        Inputs
        ======================
        uri: str - the URI from MyTardis
        ======================
        Returns
        ======================
        resource_id: int - the integer id that maps to the URI
        """
        resource_id = int(urlparse(uri).path.rstrip(
            os.sep).split(os.sep).pop())
        return resource_id

    def get_uri(self,
                action,
                search_target,
                search_string):
        """
        General solution for finding resource uri's from the 
        database.
        #
        Inputs:
        =================================
        action: the object being retrieved
        search_target: str - key to search within
        search_string: str - value to search for
        #
        Returns:
        =================================
        URI if one object with the search name exists
        None if no objects with the search name exist
        UnableToFindUniqueError if multiple objects are returned
        """
        query_params = {search_target: search_string}
        try:
            response = self.get_request(action,
                                        query_params)
        except Exception as error:
            raise error
        else:
            response_dict = json.loads(response.text)
            if response_dict == {} or response_dict['objects'] == []:
                return (None, None)
            elif len(response_dict['objects']) > 1:
                raise UnableToFindUniqueError(action,
                                              query_params,
                                              response_dict)
            else:
                obj = response_dict['objects'][0]
                uri = obj['resource_uri']
                return (uri, obj)

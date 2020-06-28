# overseer.py
#
# Abstract class definition for the MyTardis overseer class.
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 04 Jun 2020
#

from abc import ABC, abstractmethod
from ..helpers import MyTardisREST
from ..helpers import RAiDFactory
from ..helpers import UnableToFindUniqueError
import logging

logger = logging.getLogger(__name__)


class MyTardisOverseer(ABC):
    '''
    Abstract observer class:
    Overseer classes inspect the MyTardis database to ensure
    that the forge is not creating existing objects and validates
    that the heirarchical structures needed are in place before
    a new object is created.
    '''

    def __init__(self,
                 global_config_filepath,
                 local_config_filepath):
        self.rest_factory = MyTardisRESTFactory(local_config_filepath)
        self.raid_factory = RAiDFactory(global_config_filepath)

    def __resource_uri_to_id(self, uri):
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

    def __get_uri(self,
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
            response = rest.get_request(action,
                                        query_params)
        except Exception as error:
            raise
        else:
            response_dict = json.loads(response.text)
            if response_dict = {} or response_dict['objects'] == []:
                return None
            elif len(response_dict['objects']) > 1:
                raise UnableToFindUniqueError(action,
                                              query_params,
                                              response_dict)
            else:
                obj = response_dict['objects'][0]
                uri = obj['resource_uri']
                return uri

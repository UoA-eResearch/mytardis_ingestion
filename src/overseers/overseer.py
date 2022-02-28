# pylint: disable=logging-fstring-interpolation
"""Defines Overseer class which is a class that mediates between MyTardis
and the forge classes."""

import json
import logging
import os
from typing import Union
from urllib.parse import urljoin, urlparse

from requests.exceptions import HTTPError

# HierarchyError,; sanitySanityCheckError,; calculate_md5sum,; read_json,; write_json,
from src.helpers import MyTardisRESTFactory

logger = logging.getLogger(__name__)

KB = 1024
MB = KB ** 2
GB = KB ** 3

SCHEMA_TYPES = {
    "project": 11,
    "experiment": 1,
    "dataset": 2,
    "datafile": 3,
    "Instrument": 5,
}


class Overseer:
    """The Overseer class mediates between MyTardis and the forge classes

    Overseer classes inspect the MyTardis database to ensure
    that the forge is not creating existing objects and validates
    that the heirarchical structures needed are in place before
    a new object is created.

    Where there are differences in the stored objects in MyTardis the behaviour
    is governed by the overwrite attribute of the overseer class

    Attributes:
        rest_factory: An instance of MyTardisRESTFactory providing access to the API
        projects_enabled: A boolean value indicating whether or not projects are in use
        object_acls_enabled: A boolean value indicating ACLs are present on all objects in the
            hierarchy
        detailed_acls_enabled: A boolean value indicating whether or not to use detailed ACLs
        identifier_enabled: A boolean value indicating if the identifier app is in use
        identifers: A list of objects that have identifiers active if the app is enabled
        profiles_enabled: A boolean value indicating if the profiles app is in use
        profiles: A list of objects that have profiles active if the app is enabled
    """

    def __init__(self, config_dict: dict) -> None:
        """Class initialisation using a configuration dictionary.

        Creates an instance of MyTardisRESTFactory to provide access to MyTardis for
        inspection of the database. During the initialisation, the Overseer uses
        the introspection API to check what apps are active and adjusts the behaviour of
        the ingestion scripts accordingly.

        Args:
            config_dict: A configuration dictionary containing the keys required to
                initialise a MyTardisRESTFactory instance.
        """
        self.rest_factory = MyTardisRESTFactory(config_dict)
        mytardis_setup = self.get_mytardis_set_up()
        self.old_acls = mytardis_setup["old_acls"]
        self.projects_enabled = mytardis_setup["projects_enabled"]
        self.objects_with_ids = None
        self.objects_with_profiles = None
        if "objects_with_ids" in mytardis_setup.keys():  # pylint: disable=C0201
            self.objects_with_ids = mytardis_setup["objects_with_ids"]
        if "objects_with_profiles" in mytardis_setup.keys():  # pylint: disable=C0201
            self.objects_with_ids = mytardis_setup["objects_with_profiles"]

    @staticmethod
    def resource_uri_to_id(uri: str) -> int:
        """Gets the id from a resource URI

        Takes resource URI like: http://example.org/api/v1/experiment/998
        and returns just the id value (998).

        Args:
            uri: str - the URI from MyTardis

        Returns:
            The integer id that maps to the URI
        """
        resource_id = int(urlparse(uri).path.rstrip(os.sep).split(os.sep).pop())
        return resource_id

    def get_mytardis_set_up(self) -> dict:
        """GETs the information held in the introspection API

        Makes a requests HTTP call to the introspection end-point and processes the returns
        into an appropriate MyTardis configuration dictionary.

        Returns:
           a configuration dictionary containing the current set up of the specific instance
           of MyTardis.
        """
        url = urljoin(self.rest_factory.api_template, "introspection")
        try:
            response = self.rest_factory.mytardis_api_request("GET", url)
        except HTTPError as error:
            logger.exception("Failed HTTP request from Overseer.get_mytardis_set_up")
            raise error
        except Exception as error:
            logger.exception("Non-HTTP exception in Overseer.get_mytardis_set_up")
            raise error
        response_dict = json.loads(response.json())
        if response_dict == {} or response_dict["objects"] == []:
            raise ValueError(
                (
                    "MyTardis introspection did not return any data when called from "
                    "Overseer.get_mytardis_set_up"
                )
            )
        if len(response_dict["objects"]) > 1:
            logger.error(
                (
                    "MyTardis introspection returned more than one object when called from "
                    "Overseer.get_mytardis_set_up\n"
                    f"Returned response was: {response_dict}"
                )
            )
            raise ValueError(
                (
                    "MyTardis introspection returned more than one object when called from "
                    "Overseer.get_mytardis_set_up"
                )
            )
        response_dict = response_dict["objects"][0]
        return_dict = {
            "old_acls": response_dict["experiment_only_acls"],
            "projects_enabled": response_dict["projects_enabled"],
        }
        if response_dict["identifiers_enabled"]:
            return_dict["objects_with_ids"] = response_dict["identified_objects"]
        if response_dict["profiles_enabled"]:
            return_dict["objects_with_profiles"] = response_dict["profiled_objects"]
        return return_dict

    def get_objects(
        self, object_type: str, search_target: str, search_string: str
    ) -> Union[list, None]:
        """Gets a list of objects matching the search parameters passed

        This function prepares a GET request via the MyTardisRESTFactory instance and returns
        a list of objects that match the search request.

        Args:
            object_type: A string representing the different object types that can be searched for
            search_target: The field that is being searched on (varies from object to object)
            search_string: The string that the search_target is being searched for.

        Returns:
            A list of objects from the search request made.

        Raises:
            HTTPError: The GET request failed for some reason
        """
        query_params = {search_target: search_string}
        url = urljoin(self.rest_factory.api_template, object_type)
        try:
            response = self.rest_factory.mytardis_api_request(
                "GET", url, params=query_params
            )
        except HTTPError:
            logger.exception(
                (
                    "Failed HTTP request from Overseer.get_objects call\n"
                    f"object_type = {object_type}\n"
                    f"search_target = {search_target}\n"
                    f"serach_string = {search_string}"
                )
            )
            return None
        except Exception as error:
            logger.exception(
                (
                    "Non-HTTP exception in Overseer.get_objects call\n"
                    f"object_type = {object_type}\n"
                    f"search_target = {search_target}\n"
                    f"serach_string = {search_string}"
                )
            )
            raise error
        response_dict = json.loads(response.json())
        if response_dict == {} or response_dict["objects"] == []:
            return None
        return response_dict["objects"]

    def get_uris(
        self, object_type: str, search_target: str, search_string: str
    ) -> Union[list, None]:
        """Calls self.get_objects() to get a list of objects matching search then extracts URIs

        This function calls the get_objects function with the parameters passed. It then takes
        the list of returned objects and extracts the URI from the dictionaries passed.

        Args:
            object_type: A string representing the different object types that can be searched for
            search_target: The field that is being searched on (varies from object to object)
            search_string: The string that the search_target is being searched for.

        Returns:
            A list of object URIs from the search request made.
        """
        return_list = []
        objects = self.get_objects(object_type, search_target, search_string)
        if objects:
            for obj in objects:
                try:
                    uri = obj["resource_uri"]
                except KeyError:
                    uri = None
                except Exception as error:
                    raise error
                return_list.append(uri)
            return return_list
        return None

    def validate_schema(self, schema: str, object_type: str) -> Union[bool, str]:
        """Validates that a schema with the name 'schema' exists and that it
        is the right type for the object passed.

        This function calls the schema model in MyTardis by the full
        name (a URL defined when the schema is defined). If the schema is found
        then it's type is compared against the object_type requesting the
        schema and if it passes, the URI to the schema is returned, else False is returned

        Args:
            schema: The string representing the URL of the schema
            object_type: the string representing the MyTardis Object that the
                schema is going to be applied to.

        Returns:
            Either the URI of the schema as string or False

        Raises:
            Exceptions from the API call for logging and further exception handling
        """
        try:
            object_type_int = SCHEMA_TYPES[object_type]
        except KeyError:
            logger.warning(f"Schema for {object_type} are not defined in MyTardis")
            return False
        except Exception as error:
            raise error
        try:
            objects = self.get_objects("schema", "namespace", schema)
        except Exception as error:
            raise error
        if objects and len(objects) == 1:
            schema_type = int(objects[0]["schema_type"])
            if schema_type == object_type_int:
                try:
                    return objects[0]["resource_uri"]
                except KeyError:
                    return False
                except Exception as error:
                    raise error
        return False

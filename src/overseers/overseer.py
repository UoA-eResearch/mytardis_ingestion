# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
import os
import re
from typing import Union
from urllib.parse import urljoin, urlparse

from pydantic import ValidationError
from requests.exceptions import HTTPError

from src.blueprints import StorageBox
from src.helpers import MyTardisRESTFactory
from src.helpers.config import AuthConfig, ConnectionConfig, IntrospectionConfig

logger = logging.getLogger(__name__)

KB = 1024
MB = KB**2
GB = KB**3

SCHEMA_TYPES = {
    "project": 11,
    "experiment": 1,
    "dataset": 2,
    "datafile": 3,
    "Instrument": 5,
}


class Overseer:
    """The Overseer class inspects MyTardis

    Overseer classes inspect the MyTardis database to ensure
    that the Forge is not creating existing objects and validates
    that the heirarchical structures needed are in place before
    a new object is created.

    Where there are differences in the stored objects in MyTardis the behaviour
    is governed by the overwrite attribute of the Forge class

    Attributes:
        rest_factory: An instance of MyTardisRESTFactory providing access to the API
    """

    def __init__(
        self,
        auth: AuthConfig,
        connection: ConnectionConfig,
        mytardis_setup: IntrospectionConfig,
    ) -> None:
        """Class initialisation using a configuration dictionary.

        Creates an instance of MyTardisRESTFactory to provide access to MyTardis for
        inspection of the database.

        Args:
            auth : AuthConfig
            Pydantic config class containing information about authenticating with a MyTardis instance
            connection : ConnectionConfig
            Pydantic config class containing information about connecting to a MyTardis instance
            mytardis_setup : IntrospectionConfig
        """
        self.rest_factory = MyTardisRESTFactory(auth, connection)
        self.mytardis_setup = mytardis_setup

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

    def get_objects(
        self,
        object_type: str,
        search_target: str,
        search_string: str,
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
            logger.warning(
                (
                    "Failed HTTP request from Overseer.get_objects call\n"
                    f"object_type = {object_type}\n"
                    f"search_target = {search_target}\n"
                    f"search_string = {search_string}"
                ),
                exc_info=True,
            )
            return None
        except Exception as error:
            logger.error(
                (
                    "Non-HTTP exception in Overseer.get_objects call\n"
                    f"object_type = {object_type}\n"
                    f"search_target = {search_target}\n"
                    f"search_string = {search_string}"
                ),
                exc_info=True,
            )
            raise error
        response_dict = response.json()
        if response_dict == {} or response_dict["objects"] == []:
            return []
        return response_dict["objects"]

    def get_uris(
        self,
        object_type: str,
        search_target: str,
        search_string: str,
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
        return_list: list = []
        objects = self.get_objects(object_type, search_target, search_string)
        if objects == []:
            return return_list
        if objects:
            for obj in objects:
                try:
                    uri = obj["resource_uri"]
                except KeyError as error:
                    logger.error(
                        (
                            "Malformed return from MyTardis. No resource_uri found for "
                            f"{object_type} searching on {search_target} with {search_string}"
                        ),
                        exc_info=True,
                    )
                    raise error
                except Exception as error:
                    raise error
                return_list.append(uri)
            return return_list
        return None

    @staticmethod
    def is_uri(uri_string: str, object_type: str) -> bool:
        """Does a simple assessment to see if the string is appropriately formatted as a
        URI for the object_type.

        Args:
            uri_string: the string to be tested as a URI
            object_type: the object type from which the URI should be generated

        Returns:
           True if the test string is formatted correctly for URIs, False otherwise
        """
        regex_pattern = rf"^/api/v1/{object_type}/\d*/$"
        try:
            if re.match(regex_pattern, uri_string):
                return True
        except TypeError:
            return False
        except Exception as error:
            raise error
        return False

    def get_uris_by_identifier(
        self, object_type: str, search_string: str
    ) -> Union[list, None]:
        """Wrapper around Overseer.get_uris that checks if the identifer app is enabled and
        that the object_type has identifiers.

        Args:
            object_type: A string representing the different object types that can be searched for
            search_string: The string that the search_target is being searched for.

        Returns:
            A list of object URIs from the search request made.
        """
        if self.mytardis_setup.objects_with_ids == []:
            logger.warning(
                (
                    "The identifiers app is not installed in the instance of MyTardis, "
                    "or there are no objects defined in OBJECTS_WITH_IDENTIFIERS in "
                    "settings.py"
                )
            )
            return None
        if (
            self.mytardis_setup.objects_with_ids
            and object_type not in self.mytardis_setup.objects_with_ids
        ):
            logger.warning(
                (
                    f"The object type, {object_type}, is not present in "
                    "OBJECTS_WITH_IDENTIFIERS defined in settings.py"
                )
            )
            return None
        return self.get_uris(object_type, "pids", search_string)


        Wrapper function that first searches on an identifier if the identifier app
        is active. Falls back to searching on the name field (or equivalent)

        Args:
            object_type: the string representaion of the Object in MyTardis
            object_id: the identifier used to search for the object

        Returns:
        The URI from MyTardis for the object searched for or None
        """
        uri = None
        if object_type in self.mytardis_setup["objects_with_ids"]:
            uri = self.get_uris_by_identifier(object_type, object_id)
        if not uri:
            try:
                uri = self.get_uris(
                    object_type, self.object_names[object_type], object_id
                )
            except KeyError:
                logger.warning(
                    f"The name of {object_type} objects has not been defined in the Overseer"
                )
                return None
        return uri

    def get_storage_box(  # pylint: disable=too-many-return-statements
        self,
        storage_box_name: str,
    ) -> StorageBox | None:
        """Helper function to get a storage box from MyTardis and to verify that there
        is a location associated with it.

        Args:
            storage_box_name: The human readable name that defines the storage box

        Returns:
            A StorageBox dataclass if the name is found in MyTardis and the storage
                box is completely defined, or None if this is not the case.
        """
        raw_storage_box = self.get_objects("storagebox", "name", storage_box_name)
        if not raw_storage_box or raw_storage_box == []:
            logger.warning(f"Unable to locate storage box called {storage_box_name}")
            return None
        if len(raw_storage_box) > 1:
            logger.warning(
                "Unable to uniquely identify the storage box based on the "
                f"name provided ({storage_box_name}). Please check with your "
                "system administrator to identify the source of the issue."
            )
            return None
        raw_storage_box = raw_storage_box[0]  # Unpack from the list
        location = None
        for option in raw_storage_box["options"]:
            try:
                key = option["key"]
            except KeyError:
                continue
            if key == "location":
                try:
                    location = option["value"]
                except KeyError:
                    logger.warning(
                        f"Storage box, {storage_box_name} is misconfigured. Missing location"
                    )
                    return None
        if location:
            try:
                name = raw_storage_box["name"]
                uri = raw_storage_box["resource_uri"]
            except KeyError:
                logger.warning(
                    f"Storage box, {storage_box_name} is misconfigured. Storage box "
                    f"gathered from MyTardis: {raw_storage_box}"
                )
                return None
            description = "No description"
            try:
                description = raw_storage_box["description"]
            except KeyError:
                logger.info(f"No description given for Storage Box, {storage_box_name}")
            try:
                storage_box = StorageBox(
                    name=name,
                    description=description,
                    uri=uri,
                    location=location,
                )
            except ValidationError:
                logger.warning(
                    (
                        f"Poorly defined Storage Box, {storage_box_name}. Please "
                        "check configuration in MyTardis"
                    ),
                    exc_info=True,
                )
                return None
            return storage_box
        return None


# Future functionality
'''    def validate_schema(self, schema: str, object_type: str) -> Union[bool, str]:
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
        return False'''

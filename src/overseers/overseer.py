# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
import os
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

from pydantic import ValidationError
from requests.exceptions import HTTPError

from src.blueprints import StorageBox
from src.blueprints.custom_data_types import URI
from src.helpers import MyTardisRESTFactory
from src.helpers.config import IntrospectionConfig
from src.helpers.enumerators import ObjectSearchDict, ObjectSearchEnum

logger = logging.getLogger(__name__)


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
        rest_factory: MyTardisRESTFactory,
        mytardis_setup: IntrospectionConfig,
    ) -> None:
        """Class initialisation using a configuration dictionary.

        Creates an instance of MyTardisRESTFactory to provide access to MyTardis for
        inspection of the database.

        Args:
            auth : AuthConfig
            Pydantic config class containing information about authenticating with a MyTardis
                instance
            connection : ConnectionConfig
            Pydantic config class containing information about connecting to a MyTardis instance
            mytardis_setup : IntrospectionConfig
        """
        self.rest_factory = rest_factory
        if mytardis_setup:
            self.projects_enabled = mytardis_setup.projects_enabled
            self.objects_with_ids = mytardis_setup.objects_with_ids

    @staticmethod
    def resource_uri_to_id(uri: URI) -> int:
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

    def __get_object_from_mytardis(
        self,
        object_type: ObjectSearchDict,
        query_params: Dict[str, str],
    ) -> Dict[str, List[Any]] | None:

        url = urljoin(self.rest_factory.api_template, object_type["url_substring"])
        try:
            response = self.rest_factory.mytardis_api_request(
                "GET", url, params=query_params
            )
        except HTTPError:
            logger.warning(
                (
                    "Failed HTTP request from Overseer.get_objects call\n"
                    f"object_type = {object_type}\n"
                    f"query_params = {query_params}"
                ),
                exc_info=True,
            )
            return None
        except Exception as error:
            logger.error(
                (
                    "Non-HTTP exception in Overseer.get_objects call\n"
                    f"object_type = {object_type}\n"
                    f"search_target = {query_params}"
                ),
                exc_info=True,
            )
            raise error
        response_dict = response.json()
        return response_dict

    def get_objects(
        self,
        object_type: ObjectSearchDict,
        search_string: str,
    ) -> List[Dict] | None:
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
        return_list: list = []
        response_dict = None
        if self.objects_with_ids and object_type["type"] in self.objects_with_ids:
            query_params = {"pids": search_string}
            response_dict = self.__get_object_from_mytardis(object_type, query_params)
            if response_dict and "objects" in response_dict.keys():
                return_list.append(*response_dict["objects"])
        query_params = {object_type["target"]: search_string}
        response_dict = self.__get_object_from_mytardis(object_type, query_params)
        if response_dict and "objects" in response_dict.keys():
            return_list.append(*response_dict["objects"])
        return list(set(return_list))

    def get_uris(
        self,
        object_type: ObjectSearchDict,
        search_string: str,
    ) -> List[URI] | None:
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
        objects = self.get_objects(object_type, search_string)
        if not objects:
            return None
        if objects:
            for obj in objects:
                try:
                    uri = URI(obj["resource_uri"])
                except KeyError as error:
                    logger.error(
                        (
                            "Malformed return from MyTardis. No resource_uri found for "
                            f"{object_type} searching with {search_string}. Object in "
                            f"question is {obj}."
                        ),
                        exc_info=True,
                    )
                    raise error
                except ValidationError as error:
                    logger.error(
                        (
                            "Malformed return from MyTardis. Unable to conform "
                            "resource_uri into URI format"
                        ),
                        exc_info=True,
                    )
                    raise error
                except Exception as error:
                    raise error
                return_list.append(uri)
            return return_list
        return None

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

        raw_storage_box = self.get_objects(
            ObjectSearchEnum.STORAGE_BOX.value, storage_box_name
        )
        if raw_storage_box and len(raw_storage_box) > 1:
            logger.warning(
                "Unable to uniquely identify the storage box based on the "
                f"name provided ({storage_box_name}). Please check with your "
                "system administrator to identify the source of the issue."
            )
            return None
        if not raw_storage_box:
            logger.warning(f"Unable to locate storage box called {storage_box_name}")
            return None
        storage_box = raw_storage_box[0]  # Unpack from the list
        location = None
        for option in storage_box["options"]:
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
                name = storage_box["name"]
                uri = URI(storage_box["resource_uri"])
            except KeyError:
                logger.warning(
                    f"Storage box, {storage_box_name} is misconfigured. Storage box "
                    f"gathered from MyTardis: {raw_storage_box}"
                )
                return None
            description = "No description"
            try:
                description = storage_box["description"]
            except KeyError:
                logger.info(f"No description given for Storage Box, {storage_box_name}")
            try:
                ret_storage_box = StorageBox(
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
            return ret_storage_box
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
            object_type_int = SCHEMA_TYPES[object_type] # Should be an Enum
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

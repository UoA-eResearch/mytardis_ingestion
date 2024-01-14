# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from pydantic import ValidationError
from requests.exceptions import HTTPError

from src.blueprints.custom_data_types import URI
from src.blueprints.storage_boxes import StorageBox
from src.config.config import IntrospectionConfig
from src.config.singleton import Singleton
from src.helpers.enumerators import ObjectSearchDict, ObjectSearchEnum
from src.helpers.mt_rest import MyTardisRESTFactory

logger = logging.getLogger(__name__)


class Overseer(metaclass=Singleton):
    """The Overseer class inspects MyTardis

    Overseer classes inspect the MyTardis database to ensure
    that the Forge is not creating existing objects and validates
    that the hierarchical structures needed are in place before
    a new object is created.

    Where there are differences in the stored objects in MyTardis the behaviour
    is governed by the overwrite attribute of the Forge class

    Attributes:
        rest_factory: An instance of MyTardisRESTFactory providing access to the API
    """

    _mytardis_setup: IntrospectionConfig | None = None

    def __init__(
        self,
        rest_factory: MyTardisRESTFactory,
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

    @property
    async def mytardis_setup(self) -> IntrospectionConfig:
        """Getter for mytardis_setup. Sends API request if self._mytardis_setup is None"""
        if self._mytardis_setup is None:
            #     async with aiohttp.ClientSession() as session:
            #         # self._mytardis_setup = await self.get_mytardis_setup(session)
            #         # NOTE: we definitely don't want to query this every time - this is a hack for prototyping
            #         return await self.get_mytardis_setup(session)

            # NOTE: we definitely don't want to query this every time - this is a hack for prototyping
            self._mytardis_setup = await self.get_mytardis_setup()

        return self._mytardis_setup

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
        uri_sep: str = "/"
        return int(urlparse(uri).path.rstrip(uri_sep).split(uri_sep).pop())

    async def _get_object_from_mytardis(
        self,
        object_type: ObjectSearchDict,
        session: aiohttp.ClientSession,
        query_params: Dict[str, str],
    ) -> Any | None:
        url = urljoin(self.rest_factory.api_template, object_type["url_substring"])
        try:
            response = await self.rest_factory.mytardis_api_request(
                "GET", url, session, params=query_params
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
        return await response.json()

    async def get_object_by_uri(
        self,
        uri: URI,
        session: aiohttp.ClientSession,
    ) -> Optional[Any]:
        """GET an object from MyTardis using the URI

        Args:
            uri (URI): The URI of the object to GET

        Raises:
            error: _description_
            error: _description_
            ValueError: _description_
            ValueError: _description_

        Returns:
            Dict[str,Any]: The object retrieved and deserialised from JSON
        """
        url = urljoin(self.rest_factory.hostname, uri)
        try:
            response = await self.rest_factory.mytardis_api_request("GET", url, session)
        except HTTPError:
            logger.warning(
                ("Failed HTTP request from Overseer.get_objects call\n" f"URI = {uri}"),
                exc_info=True,
            )
            return None
        except Exception as error:
            logger.error(
                ("Non-HTTP exception in Overseer.get_objects call\n" f"URI = {uri}"),
                exc_info=True,
            )
            raise error
        return await response.json()

    # TODO pylint:disable=fixme
    # we might want to add a get_objects function that let's you search for
    # specific query param combinations. Right now it checks if the search
    # string is in any of the object_type["target"] and "identifier" fields but often
    # we know where those should be found, i.e. when we pass in a search_string
    # we know it's either a pid, alternative_id or name
    async def get_objects(
        self,
        object_type: ObjectSearchDict,
        session: aiohttp.ClientSession,
        search_string: str,
    ) -> List[Dict[str, Any]]:
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
        return_list = []
        response_dict = None

        setup = await self.mytardis_setup

        if (  # pylint: disable=unsupported-membership-test
            setup.objects_with_ids and object_type["type"] in setup.objects_with_ids
        ):
            query_params = {"identifier": search_string}
            response_dict = await self._get_object_from_mytardis(
                object_type, session, query_params
            )
            if response_dict and "objects" in response_dict.keys():
                return_list.extend(iter(response_dict["objects"]))
        query_params = {object_type["target"]: search_string}
        response_dict = await self._get_object_from_mytardis(
            object_type, session, query_params
        )
        if response_dict and "objects" in response_dict.keys():
            return_list.extend(iter(response_dict["objects"]))
        new_list = []
        for obj in return_list:
            if obj not in new_list:
                new_list.append(obj)
        return new_list

    async def get_objects_by_fields(
        self,
        object_type: ObjectSearchDict,
        session: aiohttp.ClientSession,
        field_values: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Query MyTardis for objects whose attributes match the values in `field_values`.

        Args:
            object_type: the type of MyTardis object to search for
            field_values: a collection of (attribute, value) pairs defining. Objects
                with these attribute values will be returned.

        Returns:
            A collection of JSON objects representing the objects matching `field_values`
        """
        response = await self._get_object_from_mytardis(
            object_type, session, field_values
        )
        if response is None:
            raise HTTPError("MyTardis object query yielded no response")
        if objects := response.get("objects"):
            return [objects]
        return []

    async def get_uris(
        self,
        object_type: ObjectSearchDict,
        session: aiohttp.ClientSession,
        search_string: str,
    ) -> List[URI]:
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
        return_list: list[URI] = []
        objects = await self.get_objects(object_type, session, search_string)
        if not objects:
            return []
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
            return_list.append(uri)
        return return_list

    async def get_storage_box(  # pylint: disable=too-many-return-statements
        self,
        storage_box_name: str,
        session: aiohttp.ClientSession,
    ) -> StorageBox | None:
        """Helper function to get a storage box from MyTardis and to verify that there
        is a location associated with it.

        Args:
            storage_box_name: The human readable name that defines the storage box

        Returns:
            A StorageBox dataclass if the name is found in MyTardis and the storage
                box is completely defined, or None if this is not the case.
        """

        storage_box_list = await self.get_objects(
            ObjectSearchEnum.STORAGE_BOX.value, session, storage_box_name
        )
        if storage_box_list and len(storage_box_list) > 1:
            logger.warning(
                "Unable to uniquely identify the storage box based on the "
                f"name provided ({storage_box_name}). Please check with your "
                "system administrator to identify the source of the issue."
            )
            return None
        if not storage_box_list:
            logger.warning(f"Unable to locate storage box called {storage_box_name}")
            return None
        storage_box = storage_box_list[0]  # Unpack from the list
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
                    f"gathered from MyTardis: {storage_box}"
                )
                return None
            try:
                description = storage_box["description"]
            except KeyError:
                logger.warning(
                    f"No description given for Storage Box, {storage_box_name}"
                )
                description = "No description"
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
        logger.warning(
            f"Storage box, {storage_box_name} is misconfigured. Missing location"
        )
        return None

    async def get_mytardis_setup(self) -> IntrospectionConfig:
        """Query introspection API

        Requests introspection info from MyTardis instance configured in connection
        """
        async with aiohttp.ClientSession() as session:
            url = urljoin(self.rest_factory.api_template, "introspection")
            response = await self.rest_factory.mytardis_api_request("GET", url, session)
            response_dict = await response.json()

        if response_dict == {} or response_dict["objects"] == []:
            logger.error(
                (
                    "MyTardis introspection did not return any data when called from "
                    "ConfigFromEnv.get_mytardis_setup"
                )
            )
            raise ValueError(
                (
                    "MyTardis introspection did not return any data when called from "
                    "ConfigFromEnv.get_mytardis_setup"
                )
            )
        if len(response_dict["objects"]) > 1:
            logger.error(
                (
                    """MyTardis introspection returned more than one object when called from
                    ConfigFromEnv.get_mytardis_setup\n
                    Returned response was: %s""",
                    response_dict,
                )
            )
            raise ValueError(
                (
                    "MyTardis introspection returned more than one object when called from "
                    "ConfigFromEnv.get_mytardis_setup"
                )
            )
        response_dict = response_dict["objects"][0]
        mytardis_setup = IntrospectionConfig(
            old_acls=response_dict["experiment_only_acls"],
            projects_enabled=response_dict["projects_enabled"],
            objects_with_ids=response_dict["identified_objects"] or None,
            objects_with_profiles=response_dict["profiled_objects"] or None,
        )
        self._mytardis_setup = mytardis_setup
        return mytardis_setup


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

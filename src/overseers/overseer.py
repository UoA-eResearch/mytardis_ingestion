# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
from typing import Any, Optional
from urllib.parse import urljoin

from pydantic import ValidationError
from requests.exceptions import HTTPError

from src.blueprints.custom_data_types import URI

# from src.blueprints.project import Project
from src.config.config import IntrospectionConfig
from src.mytardis_client.enumerators import (
    MyTardisObject,
    MyTardisObjectType,
    get_endpoint,
)
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.object_matchers import extract_match_keys, identifier_match_keys
from src.utils.types.singleton import Singleton

logger = logging.getLogger(__name__)


MYTARDIS_PROJECTS_DISABLED_MESSAGE = (
    "MyTardis is not currently set up to use projects. Please check settings.py "
    "and ensure that the 'projects' app is enabled. This may require rerunning "
    "migrations."
)


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
    def mytardis_setup(self) -> IntrospectionConfig:
        """Getter for mytardis_setup. Sends API request if self._mytardis_setup is None"""
        return self._mytardis_setup or self.get_mytardis_setup()

    def _get_matches_from_mytardis(
        self,
        object_type: MyTardisObjectType,
        query_params: dict[str, str],
    ) -> list[dict[str, Any]]:
        endpoint = get_endpoint(object_type)
        url = urljoin(self.rest_factory.api_template, endpoint.url_suffix)
        try:
            response = self.rest_factory.mytardis_api_request(
                "GET", url, params=query_params
            )
        except HTTPError as error:
            logger.warning(
                (
                    "Failed HTTP request from Overseer.get_objects call\n"
                    f"object_type = {object_type}\n"
                    f"query_params = {query_params}"
                ),
                exc_info=True,
            )
            raise error
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

        response_json: dict[str, Any] = response.json()
        return list(response_json["objects"])

    def get_object_by_uri(
        self,
        uri: URI,
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
            response = self.rest_factory.mytardis_api_request("GET", url)
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
        return response.json()

    def get_objects_by_identifier(
        self, object_type: MyTardisObjectType, identifier: str
    ) -> list[dict[str, Any]]:
        """Retrieve objects from MyTardis of the given type with the given identifier"""
        if self.mytardis_setup.objects_with_ids is None:
            raise ValueError(
                "MyTardis instance does not support identifier search for any object types"
            )
        # pylint: disable=unsupported-membership-test
        if (
            MyTardisObject(object_type.value)
            not in self.mytardis_setup.objects_with_ids
        ):
            raise ValueError(
                f"Object type {object_type} does not support identifier search"
            )

        if objects := self._get_matches_from_mytardis(
            object_type, {"identifier": identifier}
        ):
            return list(objects)
        return []

    def get_matching_objects(
        self,
        object_type: MyTardisObjectType,
        object_data: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Retrieve objects from MyTardis with field values matching the ones in "field_values"

        The function extracts the type-dependent match keys from 'object_data' and uses them to
        query MyTardis for objects whose attributes match the match keys.
        """

        # Temporary conversion until we can unify MyTardisObjectType and MyTardisObject
        objects_with_ids = list(
            map(MyTardisObjectType, self.mytardis_setup.objects_with_ids or [])
        )

        if object_type in objects_with_ids:
            for match_keys in identifier_match_keys(object_data):
                if objects := self._get_matches_from_mytardis(object_type, match_keys):
                    return list(objects)

        match_keys_for_type = extract_match_keys(object_type, object_data)
        if objects := self._get_matches_from_mytardis(object_type, match_keys_for_type):
            return list(objects)

        return []

    def get_objects_by_name(
        self, object_type: MyTardisObjectType, name: str
    ) -> list[dict[str, Any]]:
        """Retrieve objects from MyTardis of the given type whose name matches 'name'.

        NOTE: for some objects, the name field is not guaranteed to be unique. In such cases, this
        function could return incorrect matches
        """

        return self.get_matching_objects(object_type, {"name": name})

    def get_uris(
        self,
        object_type: MyTardisObjectType,
        match_keys: dict[str, Any],
    ) -> list[URI]:
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
        objects = self._get_matches_from_mytardis(object_type, match_keys)
        if not objects:
            return []

        return_list: list[URI] = []

        for obj in objects:
            try:
                uri = URI(obj["resource_uri"])
            except KeyError as error:
                logger.error(
                    (
                        "Malformed return from MyTardis. No resource_uri found for "
                        f"{object_type} searching with {match_keys}. Object in "
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

    def get_uris_by_identifier(
        self, object_type: MyTardisObjectType, identifier: str
    ) -> list[URI]:
        """Get URIs for objects of the given type with the given identifier"""
        if self.mytardis_setup.objects_with_ids is None:
            raise ValueError(
                "MyTardis instance does not support identifier search for any object types"
            )
        # pylint: disable=unsupported-membership-test
        if (
            MyTardisObject(object_type.value)
            not in self.mytardis_setup.objects_with_ids
        ):
            raise ValueError(
                f"Object type {object_type} does not support identifier search"
            )
        return self.get_uris(object_type, {"identifier": identifier})

    def get_mytardis_setup(self) -> IntrospectionConfig:
        """Query introspection API

        Requests introspection info from MyTardis instance configured in connection
        """
        url = urljoin(self.rest_factory.api_template, "introspection")
        response = self.rest_factory.mytardis_api_request("GET", url)

        response_dict = response.json()
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

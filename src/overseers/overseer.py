# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
from collections.abc import Generator
from typing import Any, Optional
from urllib.parse import urljoin

from pydantic import ValidationError
from requests.exceptions import HTTPError

from src.blueprints.custom_data_types import URI

# from src.blueprints.project import Project
from src.config.config import IntrospectionConfig
from src.mytardis_client.endpoints import get_endpoint
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.mytardis_client.types import MyTardisObject, get_type_info
from src.utils.functional import map_optional
from src.utils.types.singleton import Singleton

logger = logging.getLogger(__name__)


MYTARDIS_PROJECTS_DISABLED_MESSAGE = (
    "MyTardis is not currently set up to use projects. Please check settings.py "
    "and ensure that the 'projects' app is enabled. This may require rerunning "
    "migrations."
)


def extract_values_for_matching(
    object_type: MyTardisObject, object_data: dict[str, Any]
) -> dict[str, Any]:
    """Extracts the values from the object_data that are used for matching the
    specified type of object in MyTardis
    """

    type_info = get_type_info(object_type)

    match_keys = {}

    for key in type_info.match_fields:
        match_keys[key] = object_data[key]

    return match_keys


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

    def type_supports_identifiers(self, object_type: MyTardisObject) -> bool:
        """Check if the given object type supports identifiers"""
        if self.mytardis_setup.objects_with_ids is None:
            return False
        # pylint: disable=unsupported-membership-test
        return object_type in self.mytardis_setup.objects_with_ids

    def generate_identifier_matchers(
        self, object_type: MyTardisObject, object_data: dict[str, Any]
    ) -> Generator[dict[str, Any], None, None]:
        """Generate object matchers for identifiers"""

        if self.mytardis_setup.objects_with_ids is None:
            raise ValueError("The MyTardis instance does not support identifier search")

        # pylint: disable=unsupported-membership-test
        if object_type not in self.mytardis_setup.objects_with_ids:
            raise ValueError(
                f"MyTardis object type '{object_type}' does not support identifier search"
            )

        if identifiers := object_data.get("identifiers"):
            for identifier in identifiers:
                yield {"identifier": identifier}

        # NOTE: if there are no identifiers, we'll yield nothing - is that an error case?

    def generate_object_matchers(
        self, object_type: MyTardisObject, object_data: dict[str, Any]
    ) -> Generator[dict[str, Any], None, None]:
        """Generate object matchers for the given object type and data"""

        if self.type_supports_identifiers(object_type):
            yield from self.generate_identifier_matchers(object_type, object_data)

        yield extract_values_for_matching(object_type, object_data)

    def _get_matches_from_mytardis(
        self,
        object_type: MyTardisObject,
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
        self, object_type: MyTardisObject, identifier: str
    ) -> list[dict[str, Any]]:
        """Retrieve objects from MyTardis of the given type with the given identifier"""

        if not self.type_supports_identifiers(object_type):
            raise ValueError(
                f"MyTardis object type '{object_type}' does not support identifier search"
            )

        if objects := self._get_matches_from_mytardis(
            object_type, {"identifier": identifier}
        ):
            return list(objects)
        return []

    def get_matching_objects(
        self,
        object_type: MyTardisObject,
        object_data: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Retrieve objects from MyTardis with field values matching the ones in "field_values"

        The function extracts the type-dependent match keys from 'object_data' and uses them to
        query MyTardis for objects whose attributes match the match keys.
        """

        matchers = self.generate_object_matchers(object_type, object_data)

        for match_keys in matchers:
            if objects := self._get_matches_from_mytardis(object_type, match_keys):
                return list(objects)

        return []

    def get_uris(
        self,
        object_type: MyTardisObject,
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
        self, object_type: MyTardisObject, identifier: str
    ) -> list[URI]:
        """Get URIs for objects of the given type with the given identifier"""

        if not self.type_supports_identifiers(object_type):
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

        objects_with_ids = map_optional(
            MyTardisObject, response_dict["identified_objects"]
        )
        objects_with_profiles = map_optional(
            MyTardisObject, response_dict["profiled_objects"]
        )

        mytardis_setup = IntrospectionConfig(
            old_acls=response_dict["experiment_only_acls"],
            projects_enabled=response_dict["projects_enabled"],
            objects_with_ids=objects_with_ids,
            objects_with_profiles=objects_with_profiles,
        )
        self._mytardis_setup = mytardis_setup
        return mytardis_setup

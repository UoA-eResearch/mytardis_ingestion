# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from pydantic import ValidationError
from requests.exceptions import HTTPError

from src.blueprints.custom_data_types import URI

# from src.blueprints.project import Project
from src.config.config import IntrospectionConfig
from src.mytardis_client.enumerators import (
    MyTardisObjectType,
    ObjectSearchDict,
    get_endpoint,
)
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.object_matchers import create_matchers
from src.utils.types.singleton import Singleton

logger = logging.getLogger(__name__)


MYTARDIS_PROJECTS_DISABLED_MESSAGE = (
    "MyTardis is not currently set up to use projects. Please check settings.py "
    "and ensure that the 'projects' app is enabled. This may require rerunning "
    "migrations."
)


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
        return resource_uri_to_id(uri)

    def _get_matches_from_mytardis(
        self,
        object_type: MyTardisObjectType,
        query_params: Dict[str, str],
    ) -> Any | None:
        endpoint = get_endpoint(object_type)
        url = urljoin(self.rest_factory.api_template, endpoint.url_suffix)
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
        return response.json()

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

    # TODO pylint:disable=fixme
    # we might want to add a get_objects function that let's you search for
    # specific query param combinations. Right now it checks if the search
    # string is in any of the object_type["target"] and "identifier" fields but often
    # we know where those should be found, i.e. when we pass in a search_string
    # we know it's either a pid, alternative_id or name
    def get_objects(
        self,
        object_type: ObjectSearchDict,
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
        mt_object_type = MyTardisObjectType(object_type["type"])

        return_list = []
        response_dict = None
        if (  # pylint: disable=unsupported-membership-test
            self.mytardis_setup.objects_with_ids
            and object_type["type"] in self.mytardis_setup.objects_with_ids
        ):
            query_params = {"identifier": search_string}
            response_dict = self._get_matches_from_mytardis(
                mt_object_type, query_params
            )
            if response_dict and "objects" in response_dict.keys():
                return_list.extend(iter(response_dict["objects"]))

        query_params = {object_type["target"]: search_string}
        response_dict = self._get_matches_from_mytardis(mt_object_type, query_params)
        if response_dict and "objects" in response_dict.keys():
            return_list.extend(iter(response_dict["objects"]))

        new_list = []
        for obj in return_list:
            if obj not in new_list:
                new_list.append(obj)
        return new_list

    def get_objects_by_fields(
        self,
        object_type: ObjectSearchDict,
        field_values: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Retrieve objects from MyTardis with field values matching the ones in "field_values"."""
        mt_object_type = MyTardisObjectType(object_type["type"])

        response = self._get_matches_from_mytardis(mt_object_type, field_values)
        if response is None:
            raise HTTPError("MyTardis object query yielded no response")

        objects: list[dict[str, Any]] | None = response.get("objects")
        if objects is None:
            return []

        return objects

    def get_matching_objects(
        self,
        object_type: MyTardisObjectType,
        field_values: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Retrieve objects from MyTardis with field values matching the ones in "field_values"."""

        objects_with_ids = list(
            map(MyTardisObjectType, self.mytardis_setup.objects_with_ids or [])
        )

        matchers = create_matchers(object_type, field_values, objects_with_ids)

        for match_key in matchers:
            response_dict = self._get_matches_from_mytardis(object_type, match_key)
            if response_dict and "objects" in response_dict.keys():
                return list(response_dict["objects"])

        return []

    def get_uris(
        self,
        object_type: ObjectSearchDict,
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
        objects = self.get_objects(object_type, search_string)
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

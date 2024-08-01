# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
from collections.abc import Generator
from typing import Any

from src.mytardis_client.endpoints import URI, MyTardisEndpoint
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.mytardis_client.objects import MyTardisObject, get_type_info
from src.mytardis_client.response_data import (
    MyTardisIntrospection,
    MyTardisResourceBase,
)
from src.utils.types.singleton import Singleton

logger = logging.getLogger(__name__)


MYTARDIS_PROJECTS_DISABLED_MESSAGE = (
    "MyTardis is not currently set up to use projects. Please check settings.py "
    "and ensure that the 'projects' app is enabled. This may require rerunning "
    "migrations."
)

_DEFAULT_ENDPOINTS: dict[MyTardisObject, MyTardisEndpoint] = {
    MyTardisObject.PROJECT: "/project",
    MyTardisObject.EXPERIMENT: "/experiment",
    MyTardisObject.DATASET: "/dataset",
    MyTardisObject.DATAFILE: "/dataset_file",
    MyTardisObject.PROJECT_PARAMETER_SET: "/projectparameterset",
    MyTardisObject.EXPERIMENT_PARAMETER_SET: "/experimentparameterset",
    MyTardisObject.DATASET_PARAMETER_SET: "/datasetparameterset",
    MyTardisObject.INSTITUTION: "/institution",
    MyTardisObject.INSTRUMENT: "/instrument",
}


def get_default_endpoint(object_type: MyTardisObject) -> MyTardisEndpoint:
    """Return the default MyTardis endpoint to POST to for the given object type.

    Note that the idea of a default endpoint is a simplification, as in general a given object
    type may be returned by multiple endpoints."""

    if endpoint := _DEFAULT_ENDPOINTS.get(object_type):
        return endpoint

    raise ValueError(f"Default endpoint not defined for object type {object_type}")


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

    _mytardis_setup: MyTardisIntrospection | None = None

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
            mytardis_setup : MyTardisIntrospection
        """
        self.rest_factory = rest_factory

    @property
    def mytardis_setup(self) -> MyTardisIntrospection:
        """Getter for mytardis_setup. Sends API request on first call and caches the result"""
        if self._mytardis_setup is None:
            self._mytardis_setup = self.fetch_mytardis_setup()
        return self._mytardis_setup

    def check_identifiers_enabled_for_type(self, object_type: MyTardisObject) -> None:
        """Check if identifiers are enabled for the given object type.

        Raises RuntimeError if the MyTardis instance doesn't have identifiers enabled.
        Raises TypeError if identifiers are not enabled for the given object type.
        """

        if not self.mytardis_setup.identifiers_enabled:
            raise RuntimeError("Identifiers are not enabled in MyTardis")

        if object_type not in self.mytardis_setup.objects_with_ids:
            raise TypeError(
                f"MyTardis does not support identifiers for object_type {object_type}"
            )

    def generate_identifier_matchers(
        self, object_type: MyTardisObject, object_data: dict[str, Any]
    ) -> Generator[dict[str, Any], None, None]:
        """Generate object matchers for identifiers"""

        self.check_identifiers_enabled_for_type(object_type)

        if identifiers := object_data.get("identifiers"):
            for identifier in identifiers:
                yield {"identifier": identifier}

    def generate_object_matchers(
        self, object_type: MyTardisObject, object_data: dict[str, Any]
    ) -> Generator[dict[str, Any], None, None]:
        """Generate object matchers for the given object type and data"""

        if object_type in self.mytardis_setup.objects_with_ids:
            yield from self.generate_identifier_matchers(object_type, object_data)

        yield extract_values_for_matching(object_type, object_data)

    def _get_matches_from_mytardis(
        self,
        object_type: MyTardisObject,
        query_params: dict[str, str],
    ) -> list[MyTardisResourceBase]:
        """Get objects from MyTardis that match the given query parameters"""

        endpoint = get_default_endpoint(object_type)

        try:
            objects, _ = self.rest_factory.get(endpoint, query_params)
        except Exception as error:
            raise RuntimeError(
                "Failed to query matching objects from MyTardis in Overseer."
                f"Object type: {object_type}. Query: {query_params}"
            ) from error

        return objects

    def get_matching_objects(
        self,
        object_type: MyTardisObject,
        object_data: dict[str, str],
    ) -> list[MyTardisResourceBase]:
        """Retrieve objects from MyTardis with field values matching the ones in "field_values"

        The function extracts the type-dependent match keys from 'object_data' and uses them to
        query MyTardis for objects whose attributes match the match keys.
        """

        matchers = self.generate_object_matchers(object_type, object_data)

        for match_keys in matchers:
            if objects := self._get_matches_from_mytardis(object_type, match_keys):
                return objects

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

        return [obj.resource_uri for obj in objects]

    def get_uris_by_identifier(
        self, object_type: MyTardisObject, identifier: str
    ) -> list[URI]:
        """Get URIs for objects of the given type with the given identifier"""

        self.check_identifiers_enabled_for_type(object_type)

        return self.get_uris(object_type, {"identifier": identifier})

    def fetch_mytardis_setup(self) -> MyTardisIntrospection:
        """Query introspection API

        Requests introspection info from MyTardis instance configured in connection
        """

        objects, _ = self.rest_factory.get("/introspection")

        if len(objects) != 1:
            raise ValueError(
                (
                    f"Expected a single object from introspection endpoint, but got {len(objects)}."
                    "MyTardis may be misconfigured. Objects returned: {objects}"
                )
            )

        introspection = objects[0]
        # Note: can we avoid the need for this check? Should be clear from the
        # endpoint literal which data type we expect.
        if not isinstance(introspection, MyTardisIntrospection):
            raise TypeError("Introspection data is not of the expected type")

        return introspection

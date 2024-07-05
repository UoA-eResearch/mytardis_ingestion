# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""Defines Overseer class which is a class that inspects MyTardis
for the Forge class."""

import logging
from collections.abc import Generator
from typing import Any, TypeVar, Union

from pydantic import BaseModel, ValidationError
from requests.exceptions import HTTPError

from src.blueprints.datafile import Datafile
from src.blueprints.dataset import Dataset
from src.blueprints.experiment import Experiment
from src.blueprints.project import Project
from src.mytardis_client.data_types import URI
from src.mytardis_client.endpoints import MyTardisEndpoint
from src.mytardis_client.mt_rest import Ingested, MyTardisRESTFactory
from src.mytardis_client.objects import MyTardisObject, get_type_info
from src.mytardis_client.response_data import MyTardisIntrospection
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


MyTardisObjectData = TypeVar("MyTardisObjectData", bound=BaseModel)

MyTardisDataclass = Union[Project, Experiment, Dataset, Datafile]


class MyTardisEndpointCache:
    """A cache for URIs and objects from a specific MyTardis endpoint"""

    def __init__(self, endpoint: MyTardisEndpoint) -> None:
        self.endpoint = endpoint
        self._objects: list[Ingested[MyTardisDataclass]] = []
        self._index: dict[dict[str, Any], int] = {}

    def emplace(self, keys: dict[str, Any], obj: Ingested[MyTardisDataclass]) -> None:
        if keys in self._index:
            raise ValueError(f"Duplicate keys {keys} in MyTardisEndpointCache")

        self._index[keys] = len(self._objects)
        self._objects.append(obj)

    def get(self, keys: dict[str, Any]) -> Ingested[MyTardisDataclass] | None:
        if object_index := self._index.get(keys):
            return self._objects[object_index]
        return None


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

        self._cache: dict[MyTardisEndpoint, MyTardisEndpointCache] = {}

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
    ) -> list[dict[str, Any]]:
        """Get objects from MyTardis that match the given query parameters"""

        endpoint = get_default_endpoint(object_type)

        try:
            response = self.rest_factory.request(
                "GET", endpoint=endpoint, params=query_params
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

    def prefetch(
        self,
        endpoint: MyTardisEndpoint,
        object_type: MyTardisDataclass,
        query_params: dict[str, Any],
    ) -> None:
        """Populate the cache for the given endpoint"""

        if self._cache.get(endpoint) is None:
            self._cache[endpoint] = MyTardisEndpointCache(endpoint)

        objects, _ = self.rest_factory.get_all(endpoint, object_type, query_params)

        def get_mt_type(dclass: type[MyTardisDataclass]) -> MyTardisObject:

            mt_type = {
                Project: MyTardisObject.PROJECT,
                Experiment: MyTardisObject.EXPERIMENT,
                Dataset: MyTardisObject.DATASET,
                Datafile: MyTardisObject.DATAFILE,
            }

            return mt_type[dclass]

        mt_type = get_mt_type(type(object_type))

        for obj in objects:
            matchers = self.generate_object_matchers(mt_type, obj.model_dump())

            for keys in matchers:
                self._cache[endpoint].emplace(keys, obj)

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

        self.check_identifiers_enabled_for_type(object_type)

        return self.get_uris(object_type, {"identifier": identifier})

    def fetch_mytardis_setup(self) -> MyTardisIntrospection:
        """Query introspection API

        Requests introspection info from MyTardis instance configured in connection
        """

        response = self.rest_factory.request("GET", "/introspection")

        response_dict = response.json()
        if response_dict == {} or response_dict["objects"] == []:
            raise ValueError(
                (
                    "MyTardis introspection did not return any data when called from "
                    "ConfigFromEnv.get_mytardis_setup"
                )
            )
        if len(response_dict["objects"]) > 1:
            raise ValueError(
                (
                    """MyTardis introspection returned more than one object when called from
                    ConfigFromEnv.get_mytardis_setup\n
                    Returned response was: %s""",
                    response_dict,
                )
            )

        introspection = MyTardisIntrospection.model_validate(
            response_dict["objects"][0]
        )

        return introspection

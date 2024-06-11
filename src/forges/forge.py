# pylint: disable=logging-fstring-interpolation
"""Defines Forge class which is a class that creates MyTardis objects."""

import logging
from typing import Any, Dict, Optional

import requests
from pydantic import ValidationError
from requests.exceptions import HTTPError, JSONDecodeError

from src.blueprints.common_models import ParameterSet
from src.blueprints.datafile import Datafile
from src.blueprints.dataset import Dataset, DatasetParameterSet
from src.blueprints.experiment import Experiment, ExperimentParameterSet
from src.blueprints.project import Project, ProjectParameterSet
from src.mytardis_client.data_types import URI, HttpRequestMethod
from src.mytardis_client.endpoints import MyTardisEndpointInfo, get_endpoint_info
from src.mytardis_client.mt_rest import BadGateWayException, MyTardisRESTFactory
from src.mytardis_client.objects import MyTardisObject
from src.overseers.overseer import get_default_endpoint

logger = logging.getLogger(__name__)


# TODO: define this in the dataclasses, once we move them to mytardis_client
def get_my_tardis_type(object_type: type) -> MyTardisObject:
    """Return the MyTardis object type based on the input object type"""
    if object_type == Project:
        return MyTardisObject.PROJECT
    if object_type == Experiment:
        return MyTardisObject.EXPERIMENT
    if object_type == Dataset:
        return MyTardisObject.DATASET
    if object_type == Datafile:
        return MyTardisObject.DATAFILE
    if object_type == ProjectParameterSet:
        return MyTardisObject.PROJECT_PARAMETER_SET
    if object_type == ExperimentParameterSet:
        return MyTardisObject.EXPERIMENT_PARAMETER_SET
    if object_type == DatasetParameterSet:
        return MyTardisObject.DATASET_PARAMETER_SET
    raise ValueError(f"Unknown object type: {object_type}")


class ForgeError(Exception):  # pylint: disable=missing-class-docstring
    pass


class Forge:
    """The Forge class creates MyTardis objects.

    The Forge class will issue a POST request to make to objects in question and
    handle any exceptions that arise gracefully.

    Attributes:
        rest_factory: An instance of MyTardisRESTFactory providing access to the API
    """

    def __init__(self, rest_factory: MyTardisRESTFactory) -> None:
        """Class initialisation using a configuration dictionary.

        Creates an instance of MyTardisRESTFactory to provide access to MyTardis for
        creating MyTardis objects.

        Args:
            auth : AuthConfig
            Pydantic config class containing information about authenticating with a MyTardis
                instance
            connection : ConnectionConfig
            Pydantic config class containing information about connecting to a MyTardis instance
        """
        self.rest_factory = rest_factory

    def _make_api_call(
        self,
        action: HttpRequestMethod,
        endpoint: MyTardisEndpointInfo,
        data: str,
    ) -> requests.Response:
        """Wrapper around a generic POST request with logging in place."""
        try:
            # response = self.rest_factory.mytardis_api_request(action, url, data=data)
            response = self.rest_factory.request(action, endpoint=endpoint, data=data)
        except (HTTPError, BadGateWayException) as error:
            message = (
                "Failed HTTP request from forge_object call\n"
                f"Url: {self.rest_factory.hostname}\nAction: {action}\nData: {data}"
            )

            logger.error(error, exc_info=True)
            raise ForgeError(
                f"Forge error during request to {self.rest_factory.hostname}. Details: {message}"
            ) from error
        except Exception as error:
            message = (
                "Non-HTTP-request-error from forge_object call\n"
                f"Url: {self.rest_factory.hostname}\nAction: {action}\nData: {data}"
            )
            logger.error(message)
            logger.error(error, exc_info=True)
            raise ForgeError(
                f"Forge error during request to {self.rest_factory.hostname}. Details: {message}"
            ) from error

        if 300 <= response.status_code < 400:
            # We don't expect any redirects with MyTardis, so treat it as an error.
            message = (
                "Object not successfully created in forge_object call\n"
                f"Url: {self.rest_factory.hostname}\nAction: {action}\nData: {data}"
                f"response status code: {response.status_code}"
            )
            logger.warning(message)
            raise ForgeError(
                f"Forge failure: request to {self.rest_factory.hostname} received a redirect response. Details: {message}"
            )

        return response

    def _get_uri_from_response(self, response_dict: Dict[str, Any]) -> URI | None:
        """Take a response dictionary parsed from the JSON return
        of a response and extract the URI from it"""
        try:
            uri = URI(response_dict["resource_uri"])
        except KeyError:
            logger.warning(
                (
                    "No URI found for newly created object in forge_object call\n"
                    f"response dictionary: {response_dict}"
                )
            )
            return None
        except ValidationError:
            logger.warning(
                (
                    "Unable to parse the resource_uri into a URI.\n"
                    f"response dictionary: {response_dict}"
                )
            )
            return None
        return uri

    def forge_object(
        self,
        refined_object: (
            Project
            | Experiment
            | Dataset
            | Datafile
            | ProjectParameterSet
            | ExperimentParameterSet
            | DatasetParameterSet
        ),
    ) -> URI | None:
        """POSTs a request to create an object in MyTardis

        This function prepares a POST request to MyTardis and catches any exceptions.
        If the overwrite_objects flag is set to True, it PUTs rather than posts

        Args:
            object_name: The name of the object for logging purposes
            object_type: The MyTardis object type to be created
            object_dict: A data dictionary to be passed to the POST request containing
                the data necessary to create the object.

        Returns:
            False if the object was not created
            The URI of the created object if it was created or updated sucessfully.

        Raises:
            HTTPError: The POST was not handled successfully
        """
        object_json = refined_object.model_dump_json(by_alias=True, exclude_none=True)
        if not object_json:
            raise ValueError(
                "Failed to serialize an object into JSON. Object passed was {refined_object}."
            )

        object_type = get_my_tardis_type(type(refined_object))
        endpoint = get_default_endpoint(object_type)
        endpoint_info = get_endpoint_info(endpoint)

        response = self._make_api_call("POST", endpoint=endpoint_info, data=object_json)

        if (
            endpoint_info.methods.POST
            and endpoint_info.methods.POST.expect_response_json
        ):
            try:
                response_dict = response.json()
            except JSONDecodeError as error:
                message = (
                    f"Expected a JSON return from the POST request "
                    "but no return was found. The object may not have "
                    "been properly created and needs investigating.\n"
                    f"Object in question is: {refined_object}"
                )
                logger.warning(message)
                raise ForgeError(message) from error

            # This check may be redundant as "expect_json" is false for parameters
            if not isinstance(refined_object, ParameterSet):
                uri = self._get_uri_from_response(response_dict)
                if not uri:
                    message = (
                        "No URI was able to be discerned when creating object: "
                        f"{refined_object.display_name}. Object may have "
                        "been successfully created in MyTardis, but needs further "
                        "investigation."
                    )
                    logger.warning(message)
                    raise ForgeError(message)

                logger.info(
                    (
                        f"Object: {refined_object.display_name} successfully "
                        "created in MyTardis\n"
                        f"Url substring: {endpoint_info.path}\n"
                    )
                )
                return uri

        # Succeeded, but we don't get a URI back for this object type
        return None

    def forge_project(
        self,
        refined_object: Project,
        project_parameters: Optional[ParameterSet],
    ) -> URI:
        """Helper function to forge a project from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the project in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            The URI of the forged project if the forging was successful, otherwise None
        """
        uri = self.forge_object(refined_object)
        if uri is None:
            raise ForgeError(
                f"No URI returned when forging project {refined_object.name}. Check MyTardis."
            )

        if project_parameters:
            refined_parameters = ProjectParameterSet(
                schema=project_parameters.parameter_schema,
                parameters=project_parameters.parameters,
                project=uri,
            )

            # No URI is yielded when forging parameters
            _ = self.forge_object(refined_parameters)

        return uri

    def forge_experiment(
        self,
        refined_object: Experiment,
        experiment_parameters: Optional[ParameterSet],
    ) -> URI:
        """Helper function to forge a project from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the project in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            The URI of the forged experiment if the forging was successful, otherwise None
        """
        uri = self.forge_object(refined_object)
        if uri is None:
            raise ForgeError(
                f"No URI returned when forging experiment {refined_object.title}. Check MyTardis."
            )

        if experiment_parameters:
            refined_parameters = ExperimentParameterSet(
                schema=experiment_parameters.parameter_schema,
                parameters=experiment_parameters.parameters,
                experiment=uri,
            )
            # No URI is yielded when forging parameters
            _ = self.forge_object(refined_parameters)

        return uri

    def forge_dataset(
        self,
        refined_object: Dataset,
        dataset_parameters: Optional[ParameterSet],
    ) -> URI:
        """Helper function to forge a project from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the project in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            The URI of the forged dataset if the forging was successful, otherwise None
        """
        uri = self.forge_object(refined_object)
        if uri is None:
            raise ForgeError(
                f"No URI returned when forging experiment ('{refined_object.description}'). "
                "Check MyTardis."
            )

        if dataset_parameters:
            refined_parameters = DatasetParameterSet(
                schema=dataset_parameters.parameter_schema,
                parameters=dataset_parameters.parameters,
                dataset=uri,
            )
            _ = self.forge_object(refined_parameters)

        # No URI yielded when forging parameters
        return uri

    def forge_datafile(
        self,
        refined_object: Datafile,
    ) -> None:
        """Helper function to forge a datafile from the combined object and parameter dictionary
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the metadata to create
                the datafile in MyTardis
        """
        # No URI is yielded when forging a datafile
        _ = self.forge_object(refined_object)

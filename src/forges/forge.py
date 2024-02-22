# pylint: disable=logging-fstring-interpolation
"""Defines Forge class which is a class that creates MyTardis objects."""

import logging
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests
from pydantic import ValidationError
from requests.exceptions import HTTPError, JSONDecodeError

from src.blueprints.common_models import ParameterSet
from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import Datafile
from src.blueprints.dataset import Dataset, DatasetParameterSet
from src.blueprints.experiment import Experiment, ExperimentParameterSet
from src.blueprints.project import Project, ProjectParameterSet
from src.helpers.dataclass import get_object_post_type
from src.mytardis_client.mt_rest import BadGateWayException, MyTardisRESTFactory

logger = logging.getLogger(__name__)


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
        url: str,
        action: str,
        data: str,
    ) -> requests.Response:
        """Wrapper around a generic POST request with logging in place."""
        try:
            response = self.rest_factory.mytardis_api_request(action, url, data=data)
        except (HTTPError, BadGateWayException) as error:
            message = (
                "Failed HTTP request from forge_object call\n"
                f"Url: {url}\nAction: {action}\nData: {data}"
            )

            logger.error(error, exc_info=True)
            raise ForgeError(
                f"Forge error during request to {url}. Details: {message}"
            ) from error
        except Exception as error:
            message = (
                "Non-HTTP-request-error from forge_object call\n"
                f"Url: {url}\nAction: {action}\nData: {data}"
            )
            logger.error(message)
            logger.error(error, exc_info=True)
            raise ForgeError(
                f"Forge error during request to {url}. Details: {message}"
            ) from error

        if 300 <= response.status_code < 400:
            # We don't expect any redirects with MyTardis, so treat it as an error.
            message = (
                "Object not successfully created in forge_object call\n"
                f"Url: {url}\nAction: {action}\nData: {data}"
                f"response status code: {response.status_code}"
            )
            logger.warning(message)
            raise ForgeError(
                f"Forge failure: request to {url} received a redirect response. Details: {message}"
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

    def forge_object(  # pylint: disable=too-many-return-statements
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
        object_id: int | None = None,
        overwrite_objects: bool = False,
    ) -> URI | None:
        """POSTs a request to create an object in MyTardis

        This function prepares a POST request to MyTardis and catches any exceptions.
        If the overwrite_objects flag is set to True, it PUTs rather than posts

        Args:
            object_name: The name of the object for logging purposes
            object_type: The MyTardis object type to be created
            object_dict: A data dictionary to be passed to the POST request containing
                the data necessary to create the object.
            object_id: The id of the object to update if the forge request is an
                overwrite_objects = True forge request.
            overwrite_objects: A bool indicating whether duplicate items should be overwritten

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

        object_type = get_object_post_type(refined_object)

        # Consider refactoring this as a function to generate the URL?
        action = "POST"
        url = urljoin(self.rest_factory.api_template, object_type["url_substring"])

        if overwrite_objects:
            if object_id:
                action = "PUT"
                url = urljoin(url, str(object_id))
            else:
                raise ValueError(
                    f"Overwrite was requested for an object of type {object_type} "
                    "called from Forge class, but there was no object_id passed with this request."
                )

        response = self._make_api_call(url, action, object_json)

        if object_type["expect_json"]:
            try:
                response_dict = response.json()
            except JSONDecodeError as error:
                message = (
                    f"Expected a JSON return from the {action} request "
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
                        f"{refined_object.object_id}. Object may have "
                        "been successfully created in MyTardis, but needs further "
                        "investigation."
                    )
                    logger.warning(message)
                    raise ForgeError(message)

                logger.info(
                    (
                        f"Object: {refined_object.object_id} successfully "
                        "created in MyTardis\n"
                        f"Url substring: {object_type['url_substring']}"
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

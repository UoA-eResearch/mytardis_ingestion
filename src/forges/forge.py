# pylint: disable=logging-fstring-interpolation
"""Defines Forge class which is a class that creates MyTardis objects."""

import logging
from typing import Any, Dict, Optional, Tuple
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
from src.helpers import BadGateWayException, MyTardisRESTFactory
from src.helpers.dataclass import get_object_name, get_object_post_type

logger = logging.getLogger(__name__)


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

    def __make_api_call(
        self,
        url: str,
        action: str,
        data: str,
    ) -> requests.Response | None:
        """Wrapper around a generic POST request with logging in place."""
        try:
            response = self.rest_factory.mytardis_api_request(action, url, data=data)
        except (HTTPError, BadGateWayException) as error:
            logger.warning(
                (
                    "Failed HTTP request from forge_object call\n"
                    f"Url: {url}\nAction: {action}\nData: {data}"
                )
            )
            logger.error(error, exc_info=True)
            return None
        except Exception as error:
            logger.error(
                (
                    "Non-HTTP request from forge_object call\n"
                    f"Url: {url}\nAction: {action}\nData: {data}"
                )
            )
            logger.error(error, exc_info=True)
            raise error
        if response.status_code >= 300 and response.status_code < 400:
            logger.warning(
                (
                    "Object not successfully created in forge_object call\n"
                    f"Url: {url}\nAction: {action}\nData: {data}"
                    f"response status code: {response.status_code}"
                )
            )
            return None
        return response

    def __get_uri_from_response(self, response_dict: Dict[str, Any]) -> URI | None:
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
        refined_object: Project
        | Experiment
        | Dataset
        | Datafile
        | ProjectParameterSet
        | ExperimentParameterSet
        | DatasetParameterSet,
        object_id: int | None = None,
        overwrite_objects: bool = False,
    ) -> URI | bool | None:
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
        object_json = refined_object.json(exclude_none=True)
        if not object_json:
            logger.warning(
                (
                    "Unable to serialize the object into JSON "
                    f"format. Object passed was {refined_object}."
                )
            )
            return None
        object_type = get_object_post_type(refined_object)
        if not object_type:
            logger.warning(
                (
                    "Trying to forge a non-forgable object. "
                    f"Object handed over was {refined_object}"
                )
            )
            return None
        # Consider refactoring this as a function to generate the URL?
        url_substring = object_type.value["url_substring"]
        action = "POST"
        url = urljoin(self.rest_factory.api_template, url_substring)
        url = url + "/"
        if overwrite_objects:
            if object_id:
                action = "PUT"
                url = urljoin(self.rest_factory.api_template, url_substring)
                url = url + "/"
                url = urljoin(url, str(object_id))
                url = url + "/"
            else:
                logger.warning(
                    (
                        f"Overwrite was requested for an object of type {object_type} "
                        "called from Forge class. There was no object_id passed with this request"
                    )
                )
                return None
        response = self.__make_api_call(url, action, object_json)
        if response and object_type.value["expect_json"]:
            try:
                response_dict = response.json()
            except JSONDecodeError:
                # Not all objects return any information when they are created
                # In this case ignore the error - Need to think about how this is
                # handled.
                logger.warning(
                    (
                        f"Expected a JSON return from the {action} request "
                        "but no return was found. The object may not have "
                        "been properly created and needs investigating.\n"
                        f"Object in question is: {refined_object}"
                    )
                )
                return None
            uri = self.__get_uri_from_response(response_dict)
            if not isinstance(refined_object, ParameterSet):
                if uri:
                    logger.info(
                        (
                            f"Object: {get_object_name(refined_object)} successfully "
                            "created in MyTardis\n"
                            f"Object Type: {object_type}"
                        )
                    )
                    return uri
                logger.warning(
                    (
                        "No URI was able to be discerned when creating object: "
                        f"{get_object_name(refined_object)}. Object may have "
                        "been successfully created in MyTardis, but needs further "
                        "investigation."
                    )
                )
                return None
        return True

    def forge_project(
        self,
        refined_object: Project,
        project_parameters: Optional[ParameterSet],
    ) -> Tuple[Optional[URI | bool], Optional[URI | bool]]:
        """Helper function to forge a project from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the project in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            a tuple containing the URI of the forged project and boolean flags indicating the
                status of the object creation and the associated parameter set creation.
        """
        uri = self.forge_object(refined_object)
        parameter_uri = None
        if isinstance(uri, URI) and project_parameters:
            refined_parameters = ProjectParameterSet(
                schema=project_parameters.parameter_schema,
                parameters=project_parameters.parameters,
                project=uri,
            )
            parameter_uri = self.forge_object(refined_parameters)
        return (uri, parameter_uri)

    def forge_experiment(
        self,
        refined_object: Experiment,
        experiment_parameters: Optional[ParameterSet],
    ) -> Tuple[Optional[URI | bool], Optional[URI | bool]]:
        """Helper function to forge a project from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the project in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            a tuple containing the URI of the forged project and boolean flags indicating the
                status of the object creation and the associated parameter set creation.
        """
        uri = self.forge_object(refined_object)
        parameter_uri = None
        if isinstance(uri, URI) and experiment_parameters:
            refined_parameters = ExperimentParameterSet(
                schema=experiment_parameters.parameter_schema,
                parameters=experiment_parameters.parameters,
                experiment=uri,
            )
            parameter_uri = self.forge_object(refined_parameters)
        return (uri, parameter_uri)

    def forge_dataset(
        self,
        refined_object: Dataset,
        dataset_parameters: Optional[ParameterSet],
    ) -> Tuple[Optional[URI | bool], Optional[URI | bool]]:
        """Helper function to forge a project from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the project in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            a tuple containing the URI of the forged project and boolean flags indicating the
                status of the object creation and the associated parameter set creation.
        """
        uri = self.forge_object(refined_object)
        parameter_uri = None
        if isinstance(uri, URI) and dataset_parameters:
            refined_parameters = DatasetParameterSet(
                schema=dataset_parameters.parameter_schema,
                parameters=dataset_parameters.parameters,
                dataset=uri,
            )
            parameter_uri = self.forge_object(refined_parameters)
        return (uri, parameter_uri)

    def forge_datafile(
        self,
        refined_object: Datafile,
    ) -> URI | bool | None:
        """Helper function to forge a datafile from the combined object and parameter dictionary
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the metadata to create
                the datafile in MyTardis

        Returns:
            a tuple containing the URI of the forged project and boolean flags indicating the
                status of the object creation.
        """
        uri = self.forge_object(refined_object)
        return uri

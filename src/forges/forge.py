# pylint: disable=logging-fstring-interpolation
"""Defines Forge class which is a class that creates MyTardis objects."""

import logging
from typing import Union
from urllib.parse import urljoin

from requests.exceptions import HTTPError, JSONDecodeError

from src.helpers import BadGateWayException, MyTardisRESTFactory, dict_to_json

logger = logging.getLogger(__name__)


class Forge:
    """The Forge class creates MyTardis objects.

    The Forge class will issue a POST request to make to objects in question and
    handle any exceptions that arise gracefully.

    Attributes:
        rest_factory: An instance of MyTardisRESTFactory providing access to the API
    """

    def __init__(self, config_dict: dict) -> None:
        """Class initialisation using a configuration dictionary.

        Creates an instance of MyTardisRESTFactory to provide access to MyTardis for
        creating MyTardis objects.

        Args:
            config_dict: A configuration dictionary containing the keys required to
                initialise a MyTardisRESTFactory instance.
        """
        self.rest_factory = MyTardisRESTFactory(config_dict)

    def forge_object(
        self,
        object_name: str,
        object_type: str,
        object_dict: dict,
        object_id: Union[int, None] = None,
        overwrite_objects: bool = False,
    ) -> tuple:
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
        object_json = dict_to_json(object_dict)
        # Consider refactoring this as a function to generate the URL?
        action = "POST"
        url = urljoin(self.rest_factory.api_template, object_type)
        url = url + "/"
        if overwrite_objects:
            if object_id:
                action = "PUT"
                url = urljoin(self.rest_factory.api_template, object_type)
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
                return (object_name, False)
        try:
            response = self.rest_factory.mytardis_api_request(
                action, url, data=object_json
            )
        except (HTTPError, BadGateWayException) as error:
            logger.warning(
                (
                    "Failed HTTP request from forge_object call\n"
                    f"object_type: {object_type}\n"
                    f"object_dict: {object_dict}\n"
                    f"object_json: {object_json}"
                )
            )
            logger.error(error, exc_info=True)
            return (object_name, False)
        except Exception as error:
            logger.error(
                (
                    "Non-HTTP request from forge_object call\n"
                    f"object_type: {object_type}\n"
                    f"object_dict: {object_dict}\n"
                    f"object_json: {object_json}"
                )
            )
            logger.error(error, exc_info=True)
            raise error
        if response.status_code >= 300 and response.status_code < 400:
            logger.warning(
                (
                    "Object not successfully created in forge_object call\n"
                    f"object_type: {object_type}\n"
                    f"object_dict: {object_dict}\n"
                    f"response status code: {response.status_code}"
                )
            )
            return (object_name, False)
        uri = None
        try:
            response_dict = response.json()
            try:
                uri = response_dict["resource_uri"]
            except KeyError:
                logger.warning(
                    (
                        "No URI found for newly created object in forge_object call\n"
                        f"object_type: {object_type}\n"
                        f"object_dict: {object_dict}\n"
                        f"response status code: {response.status_code}\n"
                        f"response text: {response.json()}"
                    )
                )
                return (object_name, False)
        except JSONDecodeError:
            # Not all objects return any information when they are created
            # In this case ignore the error - Need to think about how this is
            # handled.
            pass
        logger.info(
            (
                f"Object: {object_name} successfully created in MyTardis\n"
                f"Object Type: {object_type}"
            )
        )
        return (object_name, True, uri)

    def forge_project(
        self,
        object_dict: dict,
        parameter_dict: dict,
    ) -> tuple:
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
        project_name = object_dict["name"]
        forged_object = self.forge_object(
            project_name,
            "project",
            object_dict,
        )
        object_uri = None
        parameter_flag = False
        if forged_object[1]:
            object_uri = forged_object[2]
            if parameter_dict["parameters"] != []:
                parameter_dict["project"] = object_uri
                forged_parameters = self.forge_object(
                    f"{project_name} - Parameters",
                    "projectparameterset",
                    parameter_dict,
                )
                parameter_flag = forged_parameters[1]
            else:
                parameter_flag = True  # Nothing was created but that was expected
        return (object_uri, forged_object[1], parameter_flag)

    def forge_experiment(
        self,
        object_dict: dict,
        parameter_dict: dict,
    ) -> tuple:
        """Helper function to forge an experiment from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the experiment in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            a tuple containing the URI of the forged experiment and boolean flags indicating the
                status of the object creation and the associated parameter set creation.
        """
        experiment_name = object_dict["title"]
        forged_object = self.forge_object(
            experiment_name,
            "experiment",
            object_dict,
        )
        object_uri = None
        parameter_flag = False
        if forged_object[1]:
            object_uri = forged_object[2]
            if parameter_dict["parameters"] != []:
                parameter_dict["experiment"] = object_uri
                forged_parameters = self.forge_object(
                    f"{experiment_name} - Parameters",
                    "experimentparameterset",
                    parameter_dict,
                )
                parameter_flag = forged_parameters[1]
            else:
                parameter_flag = True  # Nothing was created but that was expected
        return (object_uri, forged_object[1], parameter_flag)

    def forge_dataset(
        self,
        object_dict: dict,
        parameter_dict: dict,
    ) -> tuple:
        """Helper function to forge a dataset from the object and parameter dictionaries
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the minimum metadata to create
                the dataset in MyTardis
            parameter_dict: The parameter dictionary containing the supplementary metadata

        Returns:
            a tuple containing the URI of the forged dataset and boolean flags indicating the
                status of the object creation and the associated parameter set creation.
        """
        dataset_name = object_dict["description"]
        forged_object = self.forge_object(
            dataset_name,
            "dataset",
            object_dict,
        )
        object_uri = None
        parameter_flag = False
        if forged_object[1]:
            object_uri = forged_object[2]
            if parameter_dict["parameters"] != []:
                parameter_dict["dataset"] = object_uri
                forged_parameters = self.forge_object(
                    f"{dataset_name} - Parameters",
                    "datasetparameterset",
                    parameter_dict,
                )
                parameter_flag = forged_parameters[1]
            else:
                parameter_flag = True  # Nothing was created but that was expected
        return (object_uri, forged_object[1], parameter_flag)

    def forge_datafile(
        self,
        object_dict: dict,
    ) -> tuple:
        """Helper function to forge a datafile from the combined object and parameter dictionary
        generated by the smelter and crucible classes

        Args:
            object_dict: The object dictionary containing the metadata to create
                the datafile in MyTardis

        Returns:
            a tuple containing the URI of the forged project and boolean flags indicating the
                status of the object creation.
        """
        datafile_name = object_dict["filename"]
        forged_object = self.forge_object(
            datafile_name,
            "dataset_file",
            object_dict,
        )
        object_uri = None
        if forged_object[1]:
            object_uri = forged_object[2]
        return (object_uri, forged_object[1])

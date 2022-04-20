# pylint: disable=logging-fstring-interpolation
"""Defines Forge class which is a class that creates MyTardis objects."""

import logging
from urllib.parse import urljoin

from requests.exceptions import HTTPError, JSONDecodeError

from src.helpers import MyTardisRESTFactory, dict_to_json

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
        object_id: int = None,
        overwrite_objects: bool = False,
    ) -> tuple:
        """POSTs a request to create an object in MyTardis

        This function prepares a POST request to MyTardis and catches any exceptions.
        If the overwrite_objects flag is set to True, it PUTs rather than posts

        Args:
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
        action = "POST"
        url = urljoin(self.rest_factory.api_template, object_type)
        url = url + "/"
        if overwrite_objects:
            if object_id:
                action = "PUT"
                url = urljoin(
                    urljoin(self.rest_factory.api_template, object_type), object_id
                )
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
        except HTTPError:
            logger.warning(
                (
                    "Failed HTTP request from Forge.forge_object call\n"
                    f"object_type: {object_type}\n"
                    f"object_dict: {object_dict}\n"
                    f"object_json: {object_json}"
                )
            )
            return (object_name, False)
        except Exception as error:
            logger.exception(
                (
                    "Non-HTTP request from Forge.forge_object call\n"
                    f"object_type: {object_type}\n"
                    f"object_dict: {object_dict}\n"
                    f"object_json: {object_json}"
                )
            )
            raise error
        if response.status_code >= 300:
            logger.warning(
                (
                    "Object not successfully created in Forge.forge_object call\n"
                    f"object_type: {object_type}\n"
                    f"object_dict: {object_dict}\n"
                    f"response status code: {response.status_code}\n"
                    f"response text: {response.json()}"
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
                        "No URI found for newly created object in Forge.forge_object call\n"
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
        return (object_name, True, uri)

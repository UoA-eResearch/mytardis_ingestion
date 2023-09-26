"""Provides a set of wrappers around the requests module

This module acts as a wrapper around the requests mdoule to make RESTful API calls to MyTardis. It
is heavily based on the NGS ingestor for MyTardis found at
    https://github.com/mytardis/mytardis_ngs_ingestor
"""

from typing import Dict, Optional
from urllib.parse import urljoin

import backoff
import requests
from requests import Response
from requests.exceptions import RequestException

from src.config.config import AuthConfig, ConnectionConfig
from src.config.singleton import Singleton


class BadGateWayException(RequestException):
    """A specific exception for 502 errors to trigger backoff retries.

    502 Bad Gateway triggers retries, since the proxy web server (eg Nginx
    or Apache) in front of MyTardis could be temporarily restarting
    """

    # Included for clarity even though it is unnecessary
    def __init__(
        self, response: Response
    ) -> None:  # pylint: disable=useless-super-delegation
        super().__init__(response)


class MyTardisRESTFactory(metaclass=Singleton):  # pylint: disable=R0903
    """Class to interact with MyTardis by calling the REST API

    This is the main class that sets up access to the RESTful API supported by MyTardis. It takes
    configuration variables as inputs into __init__ and then wraps a series of requests calls.

    Attributes:
        user_agent_name: The module __name__ used to identify the agent that is making the request
        user_agent_url: A URL to this module's github repo as a source of the user agent
        auth: A AuthConfig instance that generates the authentication header for the user
        api_template: A string containing the stub of the URL tailored for the API calls as defined
            in MyTardis
        proxies: A dictionary containing HTTP(s) proxy addresses when necessary
        verify_certificate: A boolean to determine if SSL certificates should be validated. True
            unless debugging"""

    user_agent_name = __name__
    user_agent_url = "https://github.com/UoA-eResearch/mytardis_ingestion.git"

    def __init__(
        self,
        auth: AuthConfig,
        connection: ConnectionConfig,
    ) -> None:
        """MyTardisRESTFactory initialisation using a configuration dictionary.

        Defines a set of class attributes that set up access to the MyTardis RESTful API. Includes
        authentication by means of a AuthConfig instance.

        Args:
            auth : AuthConfig
            Pydantic config class containing information about authenticating with a MyTardis
                 instance
            connection : ConnectionConfig
            Pydantic config class containing information about connecting to a MyTardis instance
        """

        self.auth = auth
        self.proxies = connection.proxy.model_dump() if connection.proxy else None
        self.hostname = connection.hostname
        self.verify_certificate = connection.verify_certificate
        self.api_template = urljoin(connection.hostname, "/api/v1/")
        self.user_agent = f"{self.user_agent_name}/2.0 ({self.user_agent_url})"

    @backoff.on_exception(backoff.expo, BadGateWayException, max_tries=8)
    def mytardis_api_request(
        self,
        method: str,  # REST api method
        url: str,
        data: Optional[str] = None,
        params: Optional[Dict[str, str]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Response:
        """Function to handle the REST API calls

        Takes a REST method and url and prepares a requests request. Once the request has been
        made, the function returns the response or raises an error if the response was not process
        properly.

        502 Bad Gateway error trigger retries, since the proxy web server (eg Nginx or Apache) in
        front of MyTardis could be temporarily restarting. This uses the @backoff decorator and is
        limited to 8 retries (can be changed in the decorator.

        Args:
            method: The REST API method, POST, GET etc.
            url: The API URL for the call requested
            data: A JSON string containing data for generating an object via POST/PUT
            params: A JSON string of parameters to be passed in the URL
            extra_headers: Extra headers (META) to be passed to the API call

        Returns:
            A requests.Response object

        Raises:
            RequestException: An error raised when the request was not able to be completed due to
                502 Bad Gateway error
            HTTPError: An error raised when the request fails for other reasons via the
                requests.Response.raise_for_status function.
        """
        if method == "POST" and url[-1] != "/":
            url = f"{url}/"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        if extra_headers:
            headers = {**headers, **extra_headers}
        if self.proxies:
            response = requests.request(
                method,
                url,
                data=data,
                params=params,
                headers=headers,
                auth=self.auth,
                verify=self.verify_certificate,
                proxies=self.proxies,
                timeout=5,
            )
        else:
            response = requests.request(
                method,
                url,
                data=data,
                params=params,
                headers=headers,
                auth=self.auth,
                verify=self.verify_certificate,
                timeout=5,
            )
        if response.status_code == 502:
            raise BadGateWayException(response)
        response.raise_for_status()

        return response

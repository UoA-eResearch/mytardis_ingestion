"""Provides a set of wrappers around the requests module

This module acts as a wrapper around the requests mdoule to make RESTful API calls to MyTardis. It
is heavily based on the NGS ingestor for MyTardis found at
    https://github.com/mytardis/mytardis_ngs_ingestor
"""

from urllib.parse import urljoin

import backoff
import requests
from requests.auth import AuthBase
from requests.exceptions import RequestException


class MyTardisAuth(AuthBase):  # pylint: disable=R0903
    """Attaches HTTP headers for Tastypie API key Authentication to the given

    Because this ingestion script will sit inside the private network and
    will act as the primary source for uploading to myTardis, authentication
    via a username and api key is used. The class functions to format the
    HTTP(S) header into an appropriate form for the MyTardis authentication
    module.

    Attributes:
        username:
            A MyTardis specific username. For the UoA instance this is usually a UPI
        api_key:
            The API key generated through MyTardis that identifies the user with username
    """

    def __init__(self, username, api_key):
        self.username = username
        self.api_key = api_key

    def __call__(self, r):
        r.headers["Authorization"] = f"ApiKey {self.username}:{self.api_key}"
        return r


class MyTardisRESTFactory:  # pylint: disable=R0903
    """Class to interact with MyTardis by calling the REST API

    This is the main class that sets up access to the RESTful API supported by MyTardis. It takes
    configuration variables as inputs into __init__ and then wraps a series of requests calls.

    Attributes:
        user_agent_name: The module __name__ used to identify the agent that is making the request
        user_agent_url: A URL to this module's github repo as a source of the user agent
        auth: A MyTardisAuth instance that generates the authentication header for the user
        api_template: A string containing the stub of the URL tailored for the API calls as defined
            in MyTardis
        proxies: A dictionary containing HTTP(s) proxy addresses when necessary
        verify_certificate: A boolean to determine if SSL certificates should be validated. True
            unless debugging"""

    user_agent_name = __name__
    user_agent_url = "https://github.com/UoA-eResearch/mytardis_ingestion.git"

    def __init__(self, config_dict: dict) -> None:
        """MyTardisRESTFactory initialisation using a configuration dictionary.

        Defines a set of class attributes that set up access to the MyTardis RESTful API. Includes
        authentication by means of a MyTardisAuth instance.

        Args:
            config_dict: A configuration dictionary containing the following keys
            hostname: The hostname that points to the specific instance of MyTardis being accessed
            username: A MyTardis specific username. For the UoA instance this is usually a UPI
            api_key: The API key generated through MyTardis that identifies the user with username
            proxy_http: A URL pointing to the HTTP proxy address, where needed. Defaults to None
            proxy_https: A URL pointing to the HTTPS proxy address, where needed. Defaults to None
            verify_certificate: A boolean to determine if SSL certificates should be validated. True
                unless debugging"""

        self.auth = MyTardisAuth(config_dict["username"], config_dict["api_key"])
        if "proxy_http" in config_dict.keys() or "proxy_https" in config_dict.keys():
            self.proxies = {
                "http": config_dict["proxy_http"],
                "https": config_dict["proxy_https"],
            }
        self.verify_certificate = config_dict["verify_certificate"]
        self.api_template = urljoin(config_dict["hostname"], "/api/v1/")
        self.user_agent = f"{self.user_agent_name}/2.0 ({self.user_agent_url})"

    @backoff.on_exception(
        backoff.expo, requests.exceptions.RequestException, max_tries=8
    )
    def mytardis_api_request(
        self,
        method: str,  # REST api method
        url: str,
        data: str = None,
        params: str = None,
        extra_headers: dict = None,
    ) -> requests.Response:
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
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        if extra_headers:
            headers = {**headers, **extra_headers}
        try:
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
                )
            if response.status_code == 502:
                error = RequestException(response)
                raise error
            response.raise_for_status()
        except Exception as error:
            raise error
        return response

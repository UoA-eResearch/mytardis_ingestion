"""Provides a set of wrappers around the requests module

This module acts as a wrapper around the requests mdoule to make RESTful API calls to MyTardis. It
is heavily based on the NGS ingestor for MyTardis found at
    https://github.com/mytardis/mytardis_ngs_ingestor
"""

from typing import Any, Dict, Generic, Literal, Optional, TypeVar
from urllib.parse import urljoin

import backoff
import requests
from pydantic import BaseModel
from requests import Response
from requests.exceptions import RequestException

from src.config.config import AuthConfig, ConnectionConfig
from src.mytardis_client.endpoints import MyTardisEndpoint
from src.utils.types.singleton import Singleton

# Defines the valid values for the MyTardis API version
MyTardisApiVersion = Literal["v1"]

HttpRequestMethod = Literal["GET", "POST", "PUT", "DELETE"]


def make_api_stub(version: MyTardisApiVersion) -> str:
    """Creates a stub for the MyTardis API URL

    Args:
        version: The version of the MyTardis API to use

    Returns:
        A string containing the stub of the URL tailored for the API calls as defined in MyTardis
    """
    return f"/api/{version}/"


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


class GetRequestMetaParams(BaseModel):
    """A Pydantic model to handle the parameters for a GET request to the MyTardis API"""

    limit: int = 20
    offset: int = 0


class GetResponseMeta(BaseModel):
    """A Pydantic model to handle the metadata from a GET request to the MyTardis API"""

    limit: int
    offset: int
    total_count: int
    next: Optional[str]
    previous: Optional[str]


MyTardisObjectData = TypeVar("MyTardisObjectData", bound=BaseModel)


class GetResponse(BaseModel, Generic[MyTardisObjectData]):
    """A Pydantic model to handle the response from a GET request to the MyTardis API"""

    meta: GetResponseMeta
    objects: list[MyTardisObjectData]


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
        self._url_base = urljoin(connection.hostname, "/api/v1/")
        self.user_agent = f"{self.user_agent_name}/2.0 ({self.user_agent_url})"
        self._session = requests.Session()

    @property
    def url_base(self) -> str:
        """The base URL for the MyTardis API calls (hostname, API version, but not the endpoint)"""
        return self._url_base

    @backoff.on_exception(backoff.expo, BadGateWayException, max_tries=8)
    def mytardis_api_request(
        self,
        method: HttpRequestMethod,
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

        response = self._session.request(
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

        if response.status_code == 502:
            raise BadGateWayException(response)
        response.raise_for_status()

        return response

    @backoff.on_exception(backoff.expo, BadGateWayException, max_tries=8)
    def request(
        self,
        method: HttpRequestMethod,
        endpoint: MyTardisEndpoint,
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
        url = urljoin(self._url_base, endpoint.url_suffix)

        if method == "POST":
            url = f"{url}/"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        if extra_headers:
            headers = {**headers, **extra_headers}

        response = self._session.request(
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

        if response.status_code == 502:
            raise BadGateWayException(response)
        response.raise_for_status()

        return response

    def get(
        self,
        endpoint: MyTardisEndpoint,
        object_type: type[MyTardisObjectData],
        query_params: dict[str, Any],
        meta_params: Optional[GetRequestMetaParams],
    ) -> tuple[list[MyTardisObjectData], GetResponseMeta]:
        """Submit a GET request to the MyTardis API and return the response as a list of objects.

        Note that the response is paginated, so the function may not return all objects matching
        'query_params'. The 'meta_params' argument can be used to control the number of results
        returned. To get all objects matching 'query_params', use the 'get_all()' method.
        """

        if meta_params is None:
            meta_params = GetRequestMetaParams(limit=10, offset=0)

        params = query_params | meta_params.model_dump()

        response_data = self.request(
            "GET",
            endpoint,
            params=params,
        )

        response_json = response_data.json()

        response_meta = GetResponseMeta.model_validate_json(response_json["meta"])
        objects = [
            object_type.model_validate_json(obj) for obj in response_json["objects"]
        ]

        return objects, response_meta

    def get_all(
        self,
        endpoint: MyTardisEndpoint,
        query_params: dict[str, Any],
        object_type: type[MyTardisObjectData],
        batch_size: int = 500,
    ) -> tuple[list[MyTardisObjectData], int]:
        """Get all objects of the given type that match 'query_params'.

        Sends repeated GET requests to the MyTardis API until all objects have been retrieved.
        The 'batch_size' argument can be used to control the number of objects retrieved in
        each request
        """

        batch_size = batch_size or 500

        objects: list[MyTardisObjectData] = []

        while True:
            request_meta = GetRequestMetaParams(limit=batch_size, offset=len(objects))

            batch_objects, response_meta = self.get(
                endpoint=endpoint,
                object_type=object_type,
                query_params=query_params,
                meta_params=request_meta,
            )

            objects.extend(batch_objects)

            last_page = len(batch_objects) < batch_size
            got_all_objects = len(objects) >= response_meta.total_count

            if last_page or got_all_objects:
                break

        return objects, response_meta.total_count

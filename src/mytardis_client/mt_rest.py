"""Provides a set of wrappers around the requests module

This module acts as a wrapper around the requests mdoule to make RESTful API calls to MyTardis. It
is heavily based on the NGS ingestor for MyTardis found at
    https://github.com/mytardis/mytardis_ngs_ingestor
"""

import logging
from copy import deepcopy
from typing import Any, Callable, Dict, Generic, Literal, Optional, TypeVar
from urllib.parse import urljoin

import requests
from pydantic import BaseModel, ValidationError
from requests import ConnectTimeout, ReadTimeout, RequestException, Response
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config.config import AuthConfig, ConnectionConfig
from src.mytardis_client.common_types import HttpRequestMethod
from src.mytardis_client.endpoint_info import get_endpoint_info
from src.mytardis_client.endpoints import URI, MyTardisEndpoint
from src.mytardis_client.response_data import MyTardisResource

# Defines the valid values for the MyTardis API version
MyTardisApiVersion = Literal["v1"]

logger = logging.getLogger(__name__)


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


class Ingested(BaseModel, Generic[MyTardisObjectData]):
    """A Pydantic model to store the data of an ingested object, i.e. the response from a GET
    request to the MyTardis API, along with the URI.
    """

    obj: MyTardisObjectData
    resource_uri: URI


def sanitize_params(params: dict[str, Any]) -> dict[str, Any]:
    """Apply any necessary cleaning/transformation to a set of query parameters for a GET request.

    Args:
        params: A dictionary of query parameters

    Returns:
        A dictionary of parameters with sanitised values.
    """

    updated_params = deepcopy(params)

    def visit_entries(obj: Any, update: Callable[[Any], Any]) -> Any:

        if isinstance(obj, dict):
            return {k: visit_entries(v, update) for k, v in obj.items()}
        if isinstance(obj, list):
            return [visit_entries(v, update) for v in obj]
        return update(obj)

    def replace_uri_with_id(value: Any) -> Any:
        if isinstance(value, URI):
            return value.id
        if isinstance(value, str):
            try:
                return URI(value).id
            except ValidationError:
                pass

        return value

    # Replace any URIs with the ID as this is what MyTardis requires for GET requests
    updated_params = visit_entries(updated_params, replace_uri_with_id)

    return updated_params


class MyTardisRESTFactory:
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
        request_timeout: int = 30,
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
        self.verify_certificate = connection.verify_certificate

        self._hostname = connection.hostname
        if not self._hostname.endswith("/"):
            self._hostname += "/"
        self._version: MyTardisApiVersion = "v1"
        self._api_stub = make_api_stub(self._version)
        self._url_base = urljoin(self._hostname, self._api_stub)

        self.user_agent = f"{self.user_agent_name}/2.0 ({self.user_agent_url})"
        self._session = requests.Session()

        self._request_timeout = request_timeout

    @property
    def hostname(self) -> str:
        """The hostname of the MyTardis instance"""
        return self._hostname

    def compose_url(self, endpoint: MyTardisEndpoint) -> str:
        """Compose a full URL from the base URL and an endpoint path."""

        path = endpoint.lstrip("/") if endpoint.startswith("/") else endpoint

        # Note: it's important here that the base URL ends with a slash and the path does not
        return urljoin(self._url_base, path)

    @retry(
        retry=retry_if_exception_type(
            (BadGateWayException, ConnectTimeout, ReadTimeout)
        ),
        wait=wait_exponential(),
        stop=stop_after_attempt(8),
        before_sleep=before_sleep_log(logger, logging.INFO),
        reraise=True,
    )
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
        url = self.compose_url(endpoint)

        if method == "GET" and params:
            params = sanitize_params(params)

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
            timeout=self._request_timeout,
        )

        if response.status_code == 502:
            raise BadGateWayException(response)
        response.raise_for_status()

        return response

    def get(
        self,
        endpoint: MyTardisEndpoint,
        query_params: Optional[dict[str, Any]] = None,
        meta_params: Optional[GetRequestMetaParams] = None,
    ) -> tuple[list[MyTardisResource], GetResponseMeta]:
        """Submit a GET request to the MyTardis API and return the response as a list of objects.

        Note that the response is paginated, so the function may not return all objects matching
        'query_params'. The 'meta_params' argument can be used to control the number of results
        returned. To get all objects matching 'query_params', use the 'get_all()' method.
        """

        endpoint_info = get_endpoint_info(endpoint)
        if endpoint_info.methods.GET is None:
            raise RuntimeError(f"GET method not supported for endpoint '{endpoint}'")

        params = query_params

        if meta_params is not None:
            params = params or {}
            params |= meta_params.model_dump()

        response_data = self.request(
            "GET",
            endpoint,
            params=params,
        )

        response_json = response_data.json()

        response_meta = GetResponseMeta.model_validate(response_json["meta"])

        objects: list[MyTardisResource] = []

        response_objects = response_json.get("objects")
        if response_objects is None:
            raise RuntimeError(
                "Ill-formed response to MyTardis GET request; response has no 'objects' list.\n"
                f"Endpoint: {endpoint}\n"
                f"Query params: {query_params}\n"
                f"Response: {response_json}"
            )

        object_type = endpoint_info.methods.GET.response_obj_type

        if not isinstance(response_objects, list):
            response_objects = [response_objects]

        for object_json in response_objects:
            obj = object_type.model_validate(object_json)
            objects.append(obj)

        return objects, response_meta

    def get_all(
        self,
        endpoint: MyTardisEndpoint,
        query_params: Optional[dict[str, Any]] = None,
        batch_size: int = 500,
    ) -> tuple[list[MyTardisResource], int]:
        """Get all objects of the given type that match 'query_params'.

        Sends repeated GET requests to the MyTardis API until all objects have been retrieved.
        The 'batch_size' argument can be used to control the number of objects retrieved in
        each request
        """

        objects: list[MyTardisResource] = []

        while True:
            request_meta = GetRequestMetaParams(limit=batch_size, offset=len(objects))

            batch_objects, response_meta = self.get(
                endpoint=endpoint,
                query_params=query_params,
                meta_params=request_meta,
            )

            objects.extend(batch_objects)

            last_page = len(batch_objects) < batch_size
            got_all_objects = len(objects) >= response_meta.total_count

            if last_page or got_all_objects:
                break

        return objects, response_meta.total_count

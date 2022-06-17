# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring

from enum import Enum
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin
from pydantic import AnyUrl, BaseModel, BaseSettings, HttpUrl, PrivateAttr
from requests import HTTPError, request
from requests.auth import AuthBase

logger = logging.getLogger(__name__)


class MyTardisAuth(BaseModel, AuthBase):  # pylint: disable=R0903
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

    username: str
    api_key: str

    def __call__(self, r):  # pylint: disable=invalid-name
        """Return an authorisation header for MyTardis"""
        r.headers["Authorization"] = f"ApiKey {self.username}:{self.api_key}"
        return r


class MyTardisProxy(BaseModel):
    http: Optional[HttpUrl] = None
    https: Optional[HttpUrl] = None


class MyTardisConnection(BaseModel):
    hostname: HttpUrl
    verify_certificate = True
    proxy: Optional[MyTardisProxy] = None
    _api_stub: str = PrivateAttr("/api/v1/")

    @property
    def api_template(self) -> str:
        return urljoin(self.hostname, self._api_stub)


class MyTardisSchema(BaseModel):
    project: Optional[AnyUrl] = None
    experiment: Optional[AnyUrl] = None
    dataset: Optional[AnyUrl] = None
    datafile: Optional[AnyUrl] = None


class MyTardisStorage(BaseModel):
    source_directory: Path
    target_directory: Path
    box: str

class MyTardisObject(str, Enum):
    # pylint: disable=invalid-name
    dataset = 'dataset'
    experiment = 'experiment'
    facility = 'facility'
    instrument = 'instrument'
    project = 'project'
    institution = 'institution'

class MyTardisIntrospection(BaseModel):
    old_acls: bool
    projects_enabled: bool
    objects_with_ids: Optional[list[MyTardisObject]]
    objects_with_profiles: Optional[list[str]]

    class Config:
        use_enum_values = True

class MyTardisSettings(BaseSettings):
    default_institution: Optional[str]
    auth: MyTardisAuth
    connection: MyTardisConnection
    storage: MyTardisStorage
    default_schema: Optional[MyTardisSchema] = None
    mytardis_setup: Optional[MyTardisIntrospection] = None

    class Config:
        env_file = '.env' # this path must be relative to the current working directory, i.e. if this class needs to be instantiated in a script running in the root directory
        env_file_encoding = 'utf-8'
        env_nested_delimiter = "__"

    def __post_init__(self):
        self.get_mytardis_setup()

    def get_mytardis_setup(self):
        user_agent = f"{__name__}/2.0 (https://github.com/UoA-eResearch/mytardis_ingestion.git)"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": user_agent,
        }
        url = urljoin(self.connection.hostname, "introspection")
        try:
            if self.connection.proxy:
                response = request(
                    "GET",
                    url,
                    headers=headers,
                    verify=self.connection.verify_certificate,
                    proxies=self.connection.proxy,
                )
            else:
                response = request(
                    "GET",
                    url,
                    headers=headers,
                    verify=self.connection.verify_certificate,
                )
        except HTTPError as error:
            # TODO add logging
            raise error
        response_dict = response.json()
        if response_dict == {} or response_dict["objects"] == []:
            logger.error(
                "MyTardis introspection did not return any data when called from "
                "MyTardisConfig.get_mytardis_set_up"
            )
            raise ValueError(
                (
                    "MyTardis introspection did not return any data when called from "
                    "MyTardisConfig.get_mytardis_set_up"
                )
            )
        if len(response_dict["objects"]) > 1:
            logger.error(
                (
                    "MyTardis introspection returned more than one object when called from "
                    "Overseer.get_mytardis_set_up\n"
                    "Returned response was: %s",
                    response_dict,
                )
            )
            raise ValueError(
                (
                    "MyTardis introspection returned more than one object when called from "
                    "Overseer.get_mytardis_set_up"
                )
            )
        response_dict = response_dict["objects"][0]
        mytardis_setup = MyTardisIntrospection(
            old_acls=response_dict["experiment_only_acls"],
            projects_enabled=response_dict["projects_enabled"],
            objects_with_ids=response_dict["identified_objects"]
            if response_dict["identified_objects"]
            else None,
            objects_with_profiles=response_dict["profiled_objects"]
            if response_dict["profiled_objects"]
            else None,
        )
        self._mytardis_setup = mytardis_setup

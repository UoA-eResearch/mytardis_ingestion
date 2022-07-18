"""
MyTardis configuration module

The purpose of this module is to provide data structures that hold information
required to carry out an ingestion into a specific MyTardis instance.

It uses Pydantic's settings system
(https://pydantic-docs.helpmanual.io/usage/settings/) to read configuration from
the environment automatically and verifies their types and values.
"""

from enum import Enum
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin
from pydantic import AnyUrl, BaseModel, BaseSettings, HttpUrl, PrivateAttr
from requests import HTTPError, request
from requests.auth import AuthBase

logger = logging.getLogger(__name__)


class MyTardisGeneral(BaseModel):
    """MyTardis general config

    Attributes:
        default_institution : Optional[str]
            name of the default institution
    """

    default_institution: Optional[str]


class MyTardisAuth(BaseModel, AuthBase):
    """Attaches HTTP headers for Tastypie API key Authentication to the given

    Because this ingestion script will sit inside the private network and will
    act as the primary source for uploading to myTardis, authentication via a
    username and api key is used. The class functions to format the HTTP(S)
    header into an appropriate form for the MyTardis authentication module.

    Attributes:
        username: str
            A MyTardis specific username. For the UoA instance this is usually a
            UPI
        api_key: str
            The API key generated through MyTardis that identifies the user with
            username
    """

    username: str
    api_key: str

    def __call__(self, r):  # pylint: disable=invalid-name
        """Return an authorisation header for MyTardis"""
        r.headers["Authorization"] = f"ApiKey {self.username}:{self.api_key}"
        return r


class MyTardisProxy(BaseModel):
    """MyTardis proxy configuration.

    Pydantic model for holding MyTardis proxy configuration.

    Attributes:
        http : Optional[HttpUrl] (default: None)
            http proxy address
        https : Optional[HttpUrl] (default: None)
            https proxy address
    """

    http: Optional[HttpUrl] = None
    https: Optional[HttpUrl] = None


class MyTardisConnection(BaseModel):
    """MyTardis connection configuration.

    Pydantic model for MyTardis connection configuration.

    Attributes:
        hostname : HttpUrl
            MyTardis instance base URL
        verify_certificate : bool (default: True)
            Checks the validity of the host certificate if `True`
        proxy : MyTardisProxy (default: None)

    Properties:
        api_template : str
            Returns the stub of the MyTardis API route
    """

    hostname: HttpUrl
    verify_certificate = True
    proxy: Optional[MyTardisProxy] = None
    _api_stub: str = PrivateAttr("/api/v1/")

    @property
    def api_template(self) -> str:
        """Appends the API stub to the configured hostname and returns it"""
        return urljoin(self.hostname, self._api_stub)


class MyTardisSchema(BaseModel):
    """MyTardis default schema configuration.

    Pydantic model for MyTardis default schema configuration.

    Attributes:
        project : Optional[AnyUrl] (default: None)
            default project schema
        experiment : Optional[AnyUrl] (default: None)
            default experiment schema
        dataset : Optional[AnyUrl] (default: None)
            default dataset schema
        datafile : Optional[AnyUrl] (default: None)
            default datafile schema
    """

    project: Optional[AnyUrl] = None
    experiment: Optional[AnyUrl] = None
    dataset: Optional[AnyUrl] = None
    datafile: Optional[AnyUrl] = None


class MyTardisStorage(BaseModel):
    """MyTardis storage configuration.

    Pydantic model for Mytardis storage configuration.

    Attributes:
        source_directory : Path
            file location on the ingestion source
        target_directory : Path
            file location on remote storage
        box : str
            name of the storage box
    """

    source_directory: Path
    target_directory: Path
    box: str


class MyTardisObject(str, Enum):
    # pylint: disable=invalid-name
    """Enum for possible MyTardis object types"""
    dataset = "dataset"
    experiment = "experiment"
    facility = "facility"
    instrument = "instrument"
    project = "project"
    institution = "institution"


class MyTardisIntrospection(BaseModel):
    """MyTardis introspection data.

    Pydantic model for MyTardis introspection data. NOTE: this class relies on
    data from the MyTardis introspection API and therefore can't be instantiated
    without a request to the specific MyTardis instance.

    Attributes:
        old_acls : bool
            the MyTardis instance uses experiment only ACLs if `True`
        projects_enabled : bool
            the MyTardis instance uses projects if `True`
        objects_with_ids : Optional[list[MyTardisObject]]
    """

    old_acls: bool
    projects_enabled: bool
    objects_with_ids: Optional[list[MyTardisObject]]
    objects_with_profiles: Optional[list[MyTardisObject]]

    class Config:  # pylint: disable=missing-class-docstring
        use_enum_values = True


class MyTardisSettings(BaseSettings):
    """Full MyTardis settings model.

    This class holds the configuration to access and run an ingestion on
    MyTardis. It also provides access to the introspection API via the
    mytardis_setup property.

    Attributes:
        general : MyTardisGeneral
            instance of Pydantic general model
        auth : MyTardisAuth
            instance of Pydantic auth model
        connection : MyTardisConnection
            instance of Pydantic connection model
        storage : MyTardisStorage
            instance of Pydantic storage model
        default_schema : Optional[MyTardisSchema] (default: None)
            instance of Pydantic schema model

    Properties:
        mytardis_setup : MyTardisIntrospection
            instance of Pydantic introspection model either from private
            attribute or new request

    ## Usage
    Requires a .env file in the current working direction:
    ```
    # Genral
    GENERAL__DEFAULT_INSTITUTION=University of Auckland
    #Auth, prefix with AUTH__
    AUTH__USERNAME=ltro982
    AUTH__API_KEY=lukas-test-key
    # Connection, prefix with CONNECTION__
    CONNECTION__HOSTNAME=https://test-mytardis.nectar.auckland.ac.nz/
    #CONNECTION__PROXY__HTTP=
    #CONNECTION__PROXY__HTTPS=
    # Storage, prefix with STORAGE__
    STORAGE__SOURCE_DIRECTORY=~/api_data
    STORAGE__TARGET_DIRECTORY=/srv/mytardis
    STORAGE__BOX=new box at /srv/mytardis
    # Schema, prefix with MYTARDIS_SCHEMA__
    # DEFAULT_SCHEMA__PROJECT=https://test.test.com
    # DEFAULT_SCHEMA__EXPERIMENT=
    # DEFAULT_SCHEMA__DATASET=
    # DEFAULT_SCHEMA__DATAFILE=
    ```
    ## Example
    ```python
    settings = MyTardisSettings()
    setup = settings.mytardis_setup # <- only has value after first call
    ```
    """

    general: Optional[MyTardisGeneral]
    auth: MyTardisAuth
    connection: MyTardisConnection
    storage: MyTardisStorage
    default_schema: Optional[MyTardisSchema] = None
    _mytardis_setup: Optional[MyTardisIntrospection] = PrivateAttr(None)

    @property
    def mytardis_setup(self) -> MyTardisIntrospection:
        """Getter for mytardis_setup. Sends API request if self._mytardis_setup is None"""
        return self._mytardis_setup or self.get_mytardis_setup()

    class Config:
        """Pydantic config to enable .env file support"""

        env_file = ".env"  # this path must be relative to the current working directory, i.e. if this class needs to be instantiated in a script running in the root directory
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"

    def get_mytardis_setup(self) -> MyTardisIntrospection:
        """Query introspection API

        Requests introspection info from MyTardis instance configured in connection
        """
        user_agent = (
            f"{__name__}/2.0 (https://github.com/UoA-eResearch/mytardis_ingestion.git)"
        )
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": user_agent,
        }
        url = urljoin(self.connection.api_template, "introspection")
        try:
            if self.connection.proxy:
                response = request(
                    "GET",
                    url,
                    headers=headers,
                    verify=self.connection.verify_certificate,
                    proxies=self.connection.proxy.dict(),
                )
            else:
                response = request(
                    "GET",
                    url,
                    headers=headers,
                    verify=self.connection.verify_certificate,
                )
            response.raise_for_status()
        except HTTPError as error:
            logger.error(
                "Introspection returned error: %s", error.response, exc_info=True
            )
            raise error
        except Exception as error:
            logger.error(
                "Non-HTTP exception in MyTardisSettings.get_mytardis_setup",
                exc_info=True,
            )
            raise error
        response_dict = response.json()
        if response_dict == {} or response_dict["objects"] == []:
            logger.error(
                "MyTardis introspection did not return any data when called from MyTardisSettings.get_mytardis_setup"
            )
            raise ValueError(
                (
                    "MyTardis introspection did not return any data when called from MyTardisSettings.get_mytardis_setup"
                )
            )
        if len(response_dict["objects"]) > 1:
            logger.error(
                (
                    """MyTardis introspection returned more than one object when called from
                    MyTardisSettings.get_mytardis_setup\n
                    Returned response was: %s""",
                    response_dict,
                )
            )
            raise ValueError(
                (
                    "MyTardis introspection returned more than one object when called from MyTardisSettings.get_mytardis_setup"
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
        return mytardis_setup

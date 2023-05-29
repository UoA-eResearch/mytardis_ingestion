# pylint: disable=line-too-long,too-few-public-methods
"""
MyTardis configuration module

The purpose of this module is to provide data structures that hold information
required to carry out an ingestion into a specific MyTardis instance.

It uses Pydantic's settings system
(https://pydantic-docs.helpmanual.io/usage/settings/) to read configuration from
the environment automatically and verifies their types and values.
"""

import logging
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urljoin

from pydantic import AnyUrl, BaseModel, BaseSettings, HttpUrl, PrivateAttr
from requests import PreparedRequest
from requests.auth import AuthBase

from src.helpers.enumerators import MyTardisObject

logger = logging.getLogger(__name__)


class GeneralConfig(BaseModel):
    """MyTardis general config

    Attributes:
        default_institution (Optional[str]): name of the default institution
        source_directory: The root directory to the data source.
    """

    default_institution: Optional[str]
    source_directory: Path


class AuthConfig(BaseModel, AuthBase):
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

    def __call__(
        self, r: PreparedRequest
    ) -> PreparedRequest:  # pylint: disable=invalid-name
        """Return an authorisation header for MyTardis"""
        r.headers["Authorization"] = f"ApiKey {self.username}:{self.api_key}"
        return r


class ProxyConfig(BaseModel):
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


class ConnectionConfig(BaseModel):
    """MyTardis connection configuration.

    Pydantic model for MyTardis connection configuration.

    Attributes:
        hostname : HttpUrl
            MyTardis instance base URL
        verify_certificate : bool (default: True)
            Checks the validity of the host certificate if `True`
        proxy : ProxyConfig (default: None)

    Properties:
        api_template : str
            Returns the stub of the MyTardis API route
    """

    hostname: HttpUrl
    verify_certificate = True
    proxy: Optional[ProxyConfig] = None
    _api_stub: str = PrivateAttr("/api/v1/")

    @property
    def api_template(self) -> str:
        """Appends the API stub to the configured hostname and returns it"""
        return urljoin(self.hostname, self._api_stub)


class SchemaConfig(BaseModel):
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


class StorageConfig(BaseModel):
    """MyTardis storage box configuration.

    Pydantic model for Mytardis storage configuration. Contains enough information
    to allow for the creation of a storage box via the project API.

    Attributes:
        target_directory (Path): file location on remote storage
        name (str): name of the storage box
    """

    storage_class: str = "django.core.files.storage.FileSystemStorage"
    options: Optional[Dict[str, str]]
    attributes: Optional[Dict[str, str]]


class IntrospectionConfig(BaseModel):
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


class ConfigFromEnv(BaseSettings):
    """Full MyTardis settings model.

    This class holds the configuration to access and run an ingestion on
    MyTardis. It also provides access to the introspection API via the
    mytardis_setup property.

    Attributes:
        general : GeneralConfig
            instance of Pydantic general model
        auth : AuthConfig
            instance of Pydantic auth model
        connection : ConnectionConfig
            instance of Pydantic connection model
        storage : StorageConfig
            instance of Pydantic storage model
        default_schema : SchemaConfig
            instance of Pydantic schema model
        archive: TimeOffsetConfig
            instance of Pydantic time offset model

    Properties:
        mytardis_setup : Optional[IntrospectionConfig] (default: None)
            instance of Pydantic introspection model either from private
            attribute or new request

    ## Usage
    Requires a .env file in the current working direction:
    ```
    # Genral
    GENERAL__DEFAULT_INSTITUTION=University of Auckland
    GENERAL__SOURCE_DIRECTORY=~/api_data
    #Auth, prefix with AUTH__
    AUTH__USERNAME=ltro982
    AUTH__API_KEY=lukas-test-key
    # Connection, prefix with CONNECTION__
    CONNECTION__HOSTNAME=https://test-mytardis.nectar.auckland.ac.nz/
    #CONNECTION__PROXY__HTTP=
    #CONNECTION__PROXY__HTTPS=
    # Storage, prefix with STORAGE__
    STORAGE__STORAGE_BOX__STORAGE_CLASS=django..core.files.storage.FileSystemStorage
    STORAGE__STORAGE_BOX__OPTIONS__KEY=value
    STORAGE__STORAGE_BOX__ATTRIBUTES__KEY=value
    # Schema, prefix with MYTARDIS_SCHEMA__
    # DEFAULT_SCHEMA__PROJECT=https://test.test.com
    # DEFAULT_SCHEMA__EXPERIMENT=
    # DEFAULT_SCHEMA__DATASET=
    # DEFAULT_SCHEMA__DATAFILE=
    # Archive, prefix with ARCHIVE__
    ARCHIVE__STORAGE_BOX__STORAGE_CLASS=django.core.files.storage.FileSystemStorage
    ARCHIVE__STORAGE_BOX__OPTIONS__KEY=value
    ARCHIVE__STORAGE_BOX__ATTRIBUTES__KEY=value
    ```
    ## Example
    ```python
    settings = ConfigFromEnv()
    setup = settings.mytardis_setup # <- only has value after first call
    ```
    """

    general: GeneralConfig
    auth: AuthConfig
    connection: ConnectionConfig
    storage: StorageConfig
    default_schema: SchemaConfig
    archive: StorageConfig

    class Config:
        """Pydantic config to enable .env file support"""

        env_file = ".env"  # this path must be relative to the current working directory, i.e. if this class needs to be instantiated in a script running in the root directory
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"

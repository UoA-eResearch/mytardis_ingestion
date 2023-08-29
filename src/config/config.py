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
from abc import ABC
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

from pydantic import BaseModel, ConfigDict, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict
from requests import PreparedRequest
from requests.auth import AuthBase

from src.blueprints.custom_data_types import MTUrl
from src.helpers.enumerators import MyTardisObject

logger = logging.getLogger(__name__)


class StorageTypesEnum(Enum):
    """An enumerator to host the different storage types that can be used by
    MyTardis"""

    FILE_SYSTEM = "file_system"
    S3 = "s3"


class GeneralConfig(BaseModel):
    """MyTardis general config

    Attributes:
        default_institution (Optional[str]): name of the default institution
        source_directory: The root directory to the data source.
    """

    default_institution: Optional[str] = None
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

    http: Optional[MTUrl] = None
    https: Optional[MTUrl] = None


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

    hostname: MTUrl
    verify_certificate: bool = True
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

    project: Optional[MTUrl] = None
    experiment: Optional[MTUrl] = None
    dataset: Optional[MTUrl] = None
    datafile: Optional[MTUrl] = None


class StorageBoxConfig(BaseModel, ABC):
    """MyTardis abstract storage box configuration.

    Pydantic model for Mytardis storage configuration. Contains enough information
    to allow for the creation of a storage box via the project API.

    Attributes:
        storage_name (str): an identifier that allows the Project to access the
            options and attributes to set up an appropriate set of storage boxes
        storage_class (str): the Django storage class
        options (dict): dictionary of storage box options NB: if the storage box
            is a file system type then this MUST contain a key 'target_root_dir'
            with a file path to the target root directory.
            if the storage box is an s3 bucket then this MUST contain a key 's3_bucket'
            with the bucket name in it.
        attributes (dict): dictionary of the storage box attributes
    """

    storage_name: str
    storage_class: StorageTypesEnum
    options: Optional[Dict[str, str]] = None
    attributes: Optional[Dict[str, str]] = None


class StorageConfig(BaseModel):
    """Default storage box information for MyTardis

    Attributes:
        active_stores (List(StorageBoxConfig)): The active storage boxes
        archives (List(StorageBoxConfig)): The archive storage boxes
    """

    active_stores: List[StorageBoxConfig]
    archives: List[StorageBoxConfig]


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
    objects_with_ids: Optional[list[MyTardisObject]] = None
    objects_with_profiles: Optional[list[MyTardisObject]] = None
    model_config = ConfigDict(use_enum_values=True)


class ProfileConfig(BaseModel):
    """Profile data for extraction plant.

class ProfileConfig(BaseModel):
    """Profile data for extraction plant.

    Pydantic model for the extraction plant profile. This profile is used to
    determine how the prospecting and mining processes are performed.
    """

    profile_name: str


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
    #profile: ProfileConfig
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_nested_delimiter="__"
    )
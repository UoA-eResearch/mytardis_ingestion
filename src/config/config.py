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
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urljoin

from pydantic import BaseModel, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict
from requests import PreparedRequest
from requests.auth import AuthBase

from src.blueprints.storage_boxes import StorageTypesEnum
from src.mytardis_client.common_types import MTUrl

logger = logging.getLogger(__name__)


class GeneralConfig(BaseModel):
    """MyTardis general config

    Attributes:
        default_institution (Optional[str]): name of the default institution
    """

    default_institution: Optional[str] = None


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
        options (dict): dictionary of storage box options.
        attributes (dict): dictionary of the storage box attributes
    """

    storage_name: str
    storage_class: StorageTypesEnum
    options: Optional[Dict[str, str | Path]] = None
    attributes: Optional[Dict[str, str]] = None


class FilesystemStorageBoxConfig(StorageBoxConfig):
    """Pydantic model for a filesystem-based MyTardis storagebox configuration.
    This is used primarily to represent the staging storagebox.
    """

    storage_class: StorageTypesEnum = StorageTypesEnum.FILE_SYSTEM
    target_root_dir: Path


class ConfigFromEnv(BaseSettings):
    """Full MyTardis settings model.

    This class holds the configuration to access and run an ingestion on
    MyTardis.

    Attributes:
        general : GeneralConfig
            instance of Pydantic general model
        auth : AuthConfig
            instance of Pydantic auth model
        connection : ConnectionConfig
            instance of Pydantic connection model
        storage : StorageBoxConfig
            instance of Pydantic storage model
        default_schema : SchemaConfig
            instance of Pydantic schema model

    ## Usage
    Requires a .env file in the current working direction:
    '''
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
    # Schema, prefix with MYTARDIS_SCHEMA__
    # DEFAULT_SCHEMA__PROJECT=https://test.test.com
    # DEFAULT_SCHEMA__EXPERIMENT=
    # DEFAULT_SCHEMA__DATASET=
    # DEFAULT_SCHEMA__DATAFILE=

    '''
    ## Example
    '''python
    settings = ConfigFromEnv()
    '''
    """

    general: GeneralConfig
    auth: AuthConfig
    connection: ConnectionConfig
    default_schema: SchemaConfig
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_nested_delimiter="__"
    )
    storage: FilesystemStorageBoxConfig

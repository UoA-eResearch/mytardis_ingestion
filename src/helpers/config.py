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
from datetime import timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from pydantic import (
    AnyUrl,
    BaseModel,
    BaseSettings,
    HttpUrl,
    PrivateAttr,
    root_validator,
)
from requests.auth import AuthBase

from src.helpers.enumerators import MyTardisObject

logger = logging.getLogger(__name__)


class GeneralConfig(BaseModel):
    """MyTardis general config

    Attributes:
        default_institution : Optional[str]
            name of the default institution
    """

    default_institution: Optional[str]


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

    def __call__(self, r):  # pylint: disable=invalid-name
        """Return an authorisation header for MyTardis"""
        r.headers["Authorization"] = f"ApiKey {self.username}:{self.api_key}"
        return r


class TimeOffsetConfig(BaseModel):
    """Configuration for the auto-archive app. This config adds a default
    time delta to be added to the ingestion date.

    Note: The default values for each of the time increments is 0. The offset
    as a whole cannot be 0, however, so the configuration validates against the
    total offset converted to days, and ensures that this is > 0.

    For the purposes of calculating the offset, a greedy approach has been taken
    for months. The total offset is determined from:
        365 days * # years +
        31 days * # months +
        7 days * # weeks +
        # days
    It is therefore possible to specify time offsets in mixed terms, such as 2
    years and 6 months.

    Attributes:
        years (int): Default = 0. The number of years in the offset
        months (int): Default = 0. The number of months in the offset
        weeks (int): Default = 0. The number of weeks in the offset
        days (int): Default = 0. The number of days in the offset
    """

    years: int = 0
    months: int = 0
    weeks: int = 0
    days: int = 0

    @root_validator(skip_on_failure=True)
    @classmethod
    def check_value_is_not_zero(cls, values):
        """Custom validator to ensure that there is at least one of:
        years, months, weeks or days."""
        days = (
            365 * values["years"]
            + 31 * values["months"]
            + 7 * values["weeks"]
            + values["days"]
        )
        if days <= 0:
            raise ValueError(
                "A time offset must be given in YEARS, MONTHS, WEEKS, or DAYS"
            )
        return values

    def __call__(self):
        days = 365 * self.years + 31 * self.months + 7 * self.weeks + self.days
        return timedelta(days=days)

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


class StorageBoxConfig(BaseModel):
    """MyTardis storage box configuration.

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

class StorageConfig(BaseModel):



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
    # Archive, prefix with ARCHIVE__
    ARCHIVE__YEARS=1
    ARCHIVE__MONTHS=18
    ARCHIVE__WEEKS=7
    ARCHIVE__DAYS=12
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
    archive: TimeOffsetConfig

    class Config:
        """Pydantic config to enable .env file support"""

        env_file = ".env"  # this path must be relative to the current working directory, i.e. if this class needs to be instantiated in a script running in the root directory
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"

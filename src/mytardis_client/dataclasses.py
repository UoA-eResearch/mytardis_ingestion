"""Dataclasses for MyTardis data.

Note that in general the dataclasses may not contain all data yielded by the MyTardis API.
The idea is to only include the data that is necessary for ingestion, to make the dataclasses
less brittle to changes in the API.
"""

from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from src.mytardis_client.endpoints import URI
from src.mytardis_client.enumerators import DataClassification
from src.mytardis_client.objects import MyTardisObject

# TODO: can we use Protocol instead of BaseModel? Only inheriting data from MyTardisResource
# TODO: use datetime instead of str for created_time, modified_time, etc.


def is_hex(value: str) -> bool:
    """Check if a string is a valid hexadecimal string."""
    try:
        _ = int(value, 16)
    except ValueError:
        return False

    return True


class MD5Sum(RootModel[str], frozen=True):
    """A string representing an MD5Sum, validated to have the correct format."""

    root: str

    def __str__(self) -> str:
        return self.root

    @field_validator("root", mode="after")
    @classmethod
    def validate_format(cls, value: str) -> str:
        """Check that the URI is well-formed"""
        if len(value) != 32 or not is_hex(value):
            raise ValidationError("MD5Sum must be a 32-character hexadecimal string")

        return value

    @field_serializer("root")
    def serialize(self, md5sum: str) -> str:
        """Serialize the MD5Sum as a simple string"""

        return md5sum


class MyTardisResource(BaseModel):
    """Base class for data retrieved from MyTardis, associated with an ingested object."""

    id: int
    resource_uri: URI


class Group(MyTardisResource):
    """Metadata associated with a group in MyTardis."""

    name: str


class Facility(MyTardisResource):
    """Metadata associated with a facility in MyTardis."""

    created_time: str  # "2023-09-26T16:42:42",
    manager_group: Group
    modified_time: str  # "2023-09-26T16:42:56.677652",
    name: str


class Institution(MyTardisResource):
    """Metadata associated with an institution in MyTardis."""

    aliases: list[str]
    identifiers: list[str]
    name: str


class Instrument(MyTardisResource):
    """Metadata associated with an instrument in MyTardis."""

    created_time: str  # "2023-09-26T16:42:32",
    facility: Facility
    modified_time: str  # "2023-09-26T16:42:58.701466",
    name: str


class MyTardisIntrospection(BaseModel):
    """MyTardis introspection data (the configuration of the MyTardis instance).

    NOTE: this class relies on data from the MyTardis introspection API and
    therefore can't be instantiated without a request to the specific MyTardis
    instance.
    """

    model_config = ConfigDict(use_enum_values=False)

    data_classification_enabled: Optional[bool]
    identifiers_enabled: bool
    objects_with_ids: list[MyTardisObject] = Field(
        validation_alias="identified_objects"
    )
    objects_with_profiles: list[MyTardisObject] = Field(
        validation_alias="profiled_objects"
    )
    old_acls: bool = Field(validation_alias="experiment_only_acls")
    projects_enabled: bool
    profiles_enabled: bool

    @model_validator(mode="after")
    def validate_consistency(self) -> Self:
        """Check that the introspection data is consistent."""

        if not self.identifiers_enabled and len(self.objects_with_ids) > 0:
            raise ValueError(
                "Identifiers are disabled in MyTardis but it reports identifiable types"
            )

        if not self.profiles_enabled and len(self.objects_with_profiles) > 0:
            raise ValueError(
                "Profiles are disabled in MyTardis but it reports profiled types"
            )

        return self


class Replica(MyTardisResource):
    """Metadata associated with a Datafile replica in MyTardis."""

    created_time: str  # "2024-04-09T11:41:29.405520",
    datafile: URI
    last_verified_time: str  # "2024-04-09T11:41:35.576550",
    location: str  # "unix_fs",
    uri: str  # "ds-544/20221113_haruna_rwm_cd34/20221113_slide3-1_humanRWM_cd34_x5_0.62umpix_03.czi",
    verified: bool


class ParameterName(MyTardisResource):
    """Schema parameter information"""

    full_name: str
    immutable: bool
    is_searchable: bool
    name: str
    parent_schema: URI = Field(serialization_alias="schema")
    sensitive: bool
    units: str


class Schema(MyTardisResource):
    """Metadata associated with a metadata schema in MyTardis."""

    hidden: bool
    immutable: bool
    name: str
    namespace: str  # TODO: URL? "http://130.216.253.65/noel-test-exp-schema",
    parameter_names: list[ParameterName]


class StorageBoxOption(MyTardisResource):
    """Data associated with a storage box option in MyTardis."""

    key: str
    storage_box: URI
    value: str
    value_type: str


class StorageBox(MyTardisResource):
    """Metadata associated with a storage box in MyTardis."""

    attributes: list[str]
    description: str
    django_storage_class: str
    max_size: Optional[int]
    name: str
    options: list[StorageBoxOption]
    status: str


class User(MyTardisResource):
    """Dataa associated with a user in MyTardis."""

    email: Optional[str]
    first_name: Optional[str]
    groups: list[Group]
    last_name: Optional[str]
    username: str  # TODO: Username


class IngestedProject(MyTardisResource):
    """Metadata associated with a project that has been ingested into MyTardis."""

    classification: DataClassification
    description: str
    identifiers: Optional[list[str]]
    institution: list[URI]
    locked: bool
    name: str
    principal_investigator: str


class IngestedExperiment(MyTardisResource):
    """Metadata associated with an experiment that has been ingested into MyTardis."""

    classification: int
    description: str
    identifiers: Optional[list[str]]
    institution_name: str
    projects: list[IngestedProject]
    title: str


class IngestedDataset(MyTardisResource):
    """Metadata associated with a dataset that has been ingested into MyTardis."""

    classification: DataClassification
    created_time: str  # TODO: make it a datetime
    description: str
    directory: str  # TODO: make it a Path?
    experiments: list[URI]
    identifiers: list[str]
    immutable: bool
    instrument: Instrument
    modified_time: str  # "2024-07-01T12:39:07.411025",
    # parameter_sets: list[Any]
    public_access: bool


class IngestedDatafile(MyTardisResource):
    """Metadata associated with a datafile that has been ingested into MyTardis."""

    created_time: str
    # "datafile": null,
    dataset: URI
    deleted: bool
    deleted_time: str
    directory: str  # TODO: make it a Path?
    filename: str
    identifiers: Optional[list[str]]
    md5sum: MD5Sum
    mimetype: str
    modification_time: str
    # "parameter_sets": []
    public_access: bool
    replicas: list[Replica]
    # sha512sum: str
    size: int
    version: int


# TODO: Introspection, various ParameterSets

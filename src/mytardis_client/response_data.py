"""Dataclasses for validating/storing MyTardis API response data."""

from abc import abstractmethod
from pathlib import Path
from typing import Any, ClassVar, Optional, Protocol  # , TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from src.mytardis_client.common_types import (
    DataClassification,
    ISODateTime,
    MD5Sum,
    MTUrl,
)
from src.mytardis_client.endpoints import URI
from src.mytardis_client.objects import MyTardisObject


class MyTardisResource(Protocol):
    """Protocol for MyTardis resources."""

    # pylint: disable=unnecessary-ellipsis
    @property
    def mytardis_type(self) -> MyTardisObject:
        """The type of the MyTardis object."""
        ...

    id: int
    resource_uri: URI


class MyTardisResourceBase(BaseModel, MyTardisResource):
    """Base class for data retrieved from MyTardis, associated with an ingested object."""

    @property
    @abstractmethod
    def mytardis_type(self) -> MyTardisObject:
        """The type of the MyTardis object."""
        raise NotImplementedError("mytardis_type must be implemented")

    id: int
    resource_uri: URI


class Group(MyTardisResourceBase):
    """Metadata associated with a group in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.GROUP

    name: str


class Facility(MyTardisResourceBase):
    """Metadata associated with a facility in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.FACILITY

    created_time: ISODateTime
    manager_group: Group
    modified_time: ISODateTime
    name: str


class Institution(MyTardisResourceBase):
    """Metadata associated with an institution in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.INSTITUTION

    aliases: Optional[list[str]]
    identifiers: list[str]
    name: str


class Instrument(MyTardisResourceBase):
    """Metadata associated with an instrument in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.INSTRUMENT

    created_time: ISODateTime
    facility: Facility
    modified_time: ISODateTime
    name: str


class MyTardisIntrospection(MyTardisResourceBase):
    """MyTardis introspection data (the configuration of the MyTardis instance).

    NOTE: this class relies on data from the MyTardis introspection API and
    therefore can't be instantiated without a request to the specific MyTardis
    instance.
    """

    model_config = ConfigDict(use_enum_values=False)

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.INTROSPECTION

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

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        """Adapter for the raw data, as the ID from the introspection endpoint
        comes back as None"""

        if isinstance(data, dict):
            dummy_id = 0
            resource_uri = data.get("resource_uri")
            if isinstance(resource_uri, str):
                data["resource_uri"] = resource_uri.replace("None", str(dummy_id))

            if "id" not in data:
                data["id"] = dummy_id

        return data

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


class ParameterName(MyTardisResourceBase):
    """Schema parameter information"""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.PARAMETER_NAME

    full_name: str
    immutable: bool
    is_searchable: bool
    name: str
    parent_schema: URI = Field(serialization_alias="schema")
    sensitive: bool
    units: str


class Replica(MyTardisResourceBase):
    """Metadata associated with a Datafile replica in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.REPLICA

    created_time: ISODateTime
    datafile: URI
    last_verified_time: Optional[ISODateTime]
    location: str
    uri: str  # Note: not a MyTardis resource URI
    verified: bool


class Schema(MyTardisResourceBase):
    """Metadata associated with a metadata schema in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.SCHEMA

    hidden: bool
    immutable: bool
    name: str
    namespace: MTUrl  # e.g. "http://130.216.253.65/noel-test-exp-schema",
    parameter_names: list[ParameterName]


class StorageBoxOption(MyTardisResourceBase):
    """Data associated with a storage box option in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.STORAGE_BOX_OPTION

    key: str
    storage_box: URI
    value: str
    value_type: str


class StorageBox(MyTardisResourceBase):
    """Metadata associated with a storage box in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.STORAGE_BOX

    attributes: list[str]
    description: str
    django_storage_class: str
    max_size: Optional[int]
    name: str
    options: list[StorageBoxOption]
    status: str


class User(MyTardisResourceBase):
    """Dataa associated with a user in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.USER

    email: Optional[str]
    first_name: Optional[str]
    groups: list[Group]
    last_name: Optional[str]
    username: str


class ProjectParameterSet(MyTardisResourceBase):
    """Metadata associated with a project parameter set in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.PROJECT_PARAMETER_SET


class ExperimentParameterSet(MyTardisResourceBase):
    """Metadata associated with an experiment parameter set in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.EXPERIMENT_PARAMETER_SET


class DatasetParameterSet(MyTardisResourceBase):
    """Metadata associated with a dataset parameter set in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.DATASET_PARAMETER_SET


class DatafileParameterSet(MyTardisResourceBase):
    """Metadata associated with a datafile parameter set in MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.DATAFILE_PARAMETER_SET


class IngestedProject(MyTardisResourceBase):
    """Metadata associated with a project that has been ingested into MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.PROJECT

    classification: DataClassification
    description: str
    identifiers: Optional[list[str]]
    institution: list[URI]
    locked: bool
    name: str
    principal_investigator: str


class IngestedExperiment(MyTardisResourceBase):
    """Metadata associated with an experiment that has been ingested into MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.EXPERIMENT

    classification: int
    description: str
    identifiers: Optional[list[str]]
    institution_name: str
    projects: list[IngestedProject]
    title: str


class IngestedDataset(MyTardisResourceBase):
    """Metadata associated with a dataset that has been ingested into MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.DATASET

    classification: DataClassification
    created_time: ISODateTime
    description: str
    directory: Path
    experiments: list[URI]
    identifiers: list[str]
    immutable: bool
    instrument: Instrument
    modified_time: ISODateTime
    parameter_sets: list[DatasetParameterSet]
    public_access: bool


class IngestedDatafile(MyTardisResourceBase):
    """Metadata associated with a datafile that has been ingested into MyTardis."""

    mytardis_type: ClassVar[MyTardisObject] = MyTardisObject.DATAFILE

    created_time: Optional[ISODateTime]
    dataset: URI
    deleted: bool
    deleted_time: Optional[ISODateTime]
    directory: Path
    filename: str
    identifiers: Optional[list[str]]
    md5sum: MD5Sum
    mimetype: str
    modification_time: Optional[ISODateTime]
    parameter_sets: list[DatafileParameterSet]
    public_access: bool
    replicas: list[Replica]
    size: int
    version: int

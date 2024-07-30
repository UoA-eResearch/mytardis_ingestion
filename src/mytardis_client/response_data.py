"""Dataclasses for validating/storing MyTardis API response data."""

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from src.mytardis_client.endpoints import URI
from src.mytardis_client.objects import MyTardisObject


class MyTardisResourceBase(BaseModel):
    """Base class for data retrieved from MyTardis, associated with an ingested object."""

    id: int
    resource_uri: URI


class MyTardisIntrospection(MyTardisResourceBase):
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

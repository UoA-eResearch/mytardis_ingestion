"""Dataclasses for validating/storing MyTardis API response data."""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from src.mytardis_client.objects import MyTardisObject

# class MyTardisIntrospection(BaseModel):
#     """MyTardis introspection data.

#     NOTE: this class relies on data from the MyTardis introspection API and therefore
#     can't be instantiated without a request to the specific MyTardis instance.
#     """

#     model_config = ConfigDict(use_enum_values=False)

#     data_classification_enabled: Optional[bool]
#     identifiers_enabled: bool
#     objects_with_ids: list[MyTardisObject] = Field(
#         validation_alias="identified_objects"
#     )
#     objects_with_profiles: list[MyTardisObject] = Field(
#         validation_alias="profiled_objects"
#     )
#     old_acls: bool = Field(validation_alias="experiment_only_acls")
#     projects_enabled: bool
#     profiles_enabled: bool

#     @model_validator(mode="after")
#     def validate_consistency(self) -> Self:
#         """Check that the introspection data is consistent."""

#         if not self.identifiers_enabled and len(self.objects_with_ids) > 0:
#             raise ValueError(
#                 "Identifiers are disabled in MyTardis but it reports identifiable types"
#             )

#         if not self.profiles_enabled and len(self.objects_with_profiles) > 0:
#             raise ValueError(
#                 "Profiles are disabled in MyTardis but it reports profiled types"
#             )

#         return self

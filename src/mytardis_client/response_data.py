"""Dataclasses for validating/storing MyTardis API response data."""

from pydantic import BaseModel, ConfigDict

from src.mytardis_client.objects import MyTardisObject


class MyTardisIntrospection(BaseModel):
    """MyTardis introspection data.

    NOTE: this class relies on data from the MyTardis introspection API and therefore
    can't be instantiated without a request to the specific MyTardis instance.
    """

    model_config = ConfigDict(use_enum_values=False)

    old_acls: bool
    projects_enabled: bool
    identifiers_enabled: bool
    profiles_enabled: bool
    objects_with_ids: list[MyTardisObject]
    objects_with_profiles: list[MyTardisObject]

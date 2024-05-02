"""
A helper for mapping fields from an RO-Crate to MyTardis object name
"""

# pylint: disable=missing-function-docstring
from src.mytardis_client.types import MyTardisObjectType


class CrateToTardisMapper:
    """
    Maps fields found in an RO-crate schema to those found in myTardis ingestion RawDataclasses
    simple lookup table

    TODO - Replace with an RO-Crate profile based schema and move to dedicated helpers/profiles .py
    """

    def __init__(self, schema: dict[str, dict[str, str]]) -> None:
        """Sets up a dictonary and reverse dictionary for mapping fields

        Args:
            schema (dict): a dictionary of mapp Ro-crate fields(key) to myTardis fields(values)
        """

        self.schema = schema
        self.reverse_schema = {
            object_key: {v: k for k, v in object_dict.items()}
            for object_key, object_dict in self.schema.items()
        }

    def get_mt_field(
        self, tardis_type: MyTardisObjectType, field_name: str
    ) -> str | None:
        return self.schema[tardis_type.name.lower()].get(field_name)

    def get_roc_field(
        self, tardis_type: MyTardisObjectType, field_name: str
    ) -> str | None:
        return self.reverse_schema[tardis_type.name.lower()].get(field_name)

    def deref_object_id(self, crate_type: str) -> str | None:
        return self.reverse_schema["derefIdentifier"].get(crate_type)

"""Definitions of attributes used to match different MyTardis object types."""

from typing import Any

from src.mytardis_client.enumerators import MyTardisObjectType
from src.overseers import resource_uri_to_id

MatchKeys = dict[str, Any]


def identifier_match_keys(object_data: dict[str, Any]) -> list[MatchKeys]:
    """Create match keys for objects which support identifiers.

    As a fallback, it looks for a name field to use as an identifier.
    """
    if identifiers := object_data.get("identifiers"):
        return [{"identifier": identifier} for identifier in identifiers]
    if name := object_data.get("name"):
        return [{"identifier": name}]

    raise ValueError("No identifiers or name found in object data")


def name_match_key(object_data: dict[str, Any]) -> MatchKeys:
    """Create a match_key that uses the name field to match objects"""
    return {
        "name": object_data["name"],
    }


def project_match_key(object_data: dict[str, Any]) -> MatchKeys:
    """Matcher which defines the project-specific match keys"""

    return {
        "name": object_data["name"],
    }


def experiment_match_key(object_data: dict[str, Any]) -> MatchKeys:
    """Matcher which defines the experiment-specific match keys"""

    return {
        "title": object_data["title"],
    }


def dataset_match_key(object_data: dict[str, Any]) -> MatchKeys:
    """Matcher which defines the dataset-specific match keys"""

    return {
        "description": object_data["description"],
        "instrument": object_data["instrument"],
    }


def datafile_match_key(object_data: dict[str, Any]) -> MatchKeys:
    """Matcher which defines the datafile-specific match keys"""

    return {
        "filename": object_data["filename"],
        "directory": object_data["directory"].as_posix(),
        "dataset": str(resource_uri_to_id(object_data["dataset"])),
    }


def extract_match_keys(
    object_type: MyTardisObjectType,
    object_data: dict[str, Any],
) -> MatchKeys:
    """Extract a match key for the given object type and data.

    The match key is a composite of one or more fields that are used to
    match against existing objects in MyTardis."""

    if object_type == MyTardisObjectType.PROJECT:
        return project_match_key(object_data)
    if object_type == MyTardisObjectType.EXPERIMENT:
        return experiment_match_key(object_data)
    if object_type == MyTardisObjectType.DATASET:
        return dataset_match_key(object_data)
    if object_type == MyTardisObjectType.DATAFILE:
        return datafile_match_key(object_data)

    return name_match_key(object_data)

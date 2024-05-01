# Provide generic version where we check if the object supports IDs, and if so, provide a matcher using identifiers, otherwise use the match keys

# Specialise it (using singledispatch?) for the objects where transformations on fields are needed


# Each thing should provide a generator of matchers, which first uses the identifiers if available, then tries the match keys

from itertools import chain
from typing import Any, Callable, Generator, Iterable

from src.mytardis_client.enumerators import MyTardisObjectType
from src.overseers import resource_uri_to_id

DataBundle = dict[str, Any]
MatchKey = dict[str, Any]
Matcher = Callable[[DataBundle], MatchKey]


def identifier_matcher(object_data: dict[str, Any]) -> Generator[MatchKey, None, None]:
    """Create a matcher that uses the identifiers field to match objects"""
    if identifiers := object_data.get("identifiers"):
        for identifier in identifiers:
            yield {"identifier": identifier}
    else:
        yield from ()


def project_matcher(object_data: dict[str, Any]) -> Generator[MatchKey, None, None]:
    """Matcher which defines the project-specific match keys"""

    yield {
        "name": object_data["name"],
    }


def experiment_matcher(object_data: dict[str, Any]) -> Generator[MatchKey, None, None]:
    """Matcher which defines the experiment-specific match keys"""

    yield {
        "title": object_data["title"],
    }


def dataset_matcher(object_data: dict[str, Any]) -> Generator[MatchKey, None, None]:
    """Matcher which defines the dataset-specific match keys"""

    yield {
        "description": object_data["description"],
    }


def datafile_matcher(object_data: dict[str, Any]) -> Generator[MatchKey, None, None]:
    """Matcher which defines the datafile-specific match keys"""

    yield {
        "filename": object_data["filename"],
        "directory": object_data["directory"].as_posix(),
        "dataset": str(resource_uri_to_id(object_data["dataset"])),
    }


def create_matchers(
    object_type: MyTardisObjectType,
    object_data: DataBundle,
    objects_with_ids: list[MyTardisObjectType],
) -> Iterable[MatchKey]:
    """Create matchers for the given object type and data

    The collection starts with a matcher for each identifier, if the
    object type supports identifiers, and the data includes identifiers.
    The last matcher uses fields specific to the object type."""

    matchers: list[Generator[MatchKey, None, None]] = []

    if object_type in objects_with_ids:
        matchers.append(identifier_matcher(object_data))

    if object_type == MyTardisObjectType.PROJECT:
        matchers.append(project_matcher(object_data))
    elif object_type == MyTardisObjectType.EXPERIMENT:
        matchers.append(experiment_matcher(object_data))
    elif object_type == MyTardisObjectType.DATASET:
        matchers.append(dataset_matcher(object_data))
    elif object_type == MyTardisObjectType.DATAFILE:
        matchers.append(datafile_matcher(object_data))

    return chain.from_iterable(matchers)

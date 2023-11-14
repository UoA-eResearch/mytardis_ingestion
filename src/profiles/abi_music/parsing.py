"""
Parsing logic for generating PEDD dataclasses from ABI Music files
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from src.blueprints.common_models import GroupACL
from src.blueprints.custom_data_types import MTUrl
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.helpers.enumerators import DataClassification

# Expected datetime format is "yymmdd-DDMMSS"
datetime_pattern = re.compile("^[0-9]{6}-[0-9]{6}$")


def parse_timestamp(timestamp: str) -> datetime:
    """
    Parse a timestamp string in the ABI Music format: yymmdd-DDMMSS

    Returns a datetime object or raises a ValueError if the string is ill-formed.
    """
    # strptime is a bit too lenient with its input format, so pre-validate with a regex
    if _ := datetime_pattern.match(timestamp):
        return datetime.strptime(timestamp, r"%y%m%d-%H%M%S")

    raise ValueError("Ill-formed timestamp; expected format 'yymmdd-DDMMSS'")


def parse_project_info(json_data: dict[str, Any]) -> RawProject:
    """
    Extract project metadata from JSON content
    """
    groups: list[GroupACL] = []

    for group in json_data["groups"]:
        name = group["name"]
        is_admin = group.get("admin") is True

        if is_admin:
            is_owner, can_download, see_sensitive = True, True, True
        else:
            is_owner = group.get("is_owner") is True
            can_download = group.get("can_download") is True
            see_sensitive = group.get("see_sensitive") is True

        group_info = GroupACL(
            group=name,
            is_owner=is_owner,
            can_download=can_download,
            see_sensitive=see_sensitive,
        )
        groups.append(group_info)

    raw_project = RawProject(
        name=json_data["project_name"],
        description=json_data["project_description"],
        principal_investigator=json_data["principal_investigator"],
        data_classification=DataClassification.SENSITIVE,
        created_by=json_data["principal_investigator"],
        users=None,
        groups=groups,
        identifiers=json_data["project_ids"],
        institution=["University of Auckland"],
        metadata=None,
        schema=None,
        start_time=None,
        end_time=None,
        embargo_until=None,
        active_stores=None,
        archives=None,
        delete_in_days=-1,
        archive_in_days=365,
    )

    return raw_project


def parse_experiment_info(json_data: dict[str, Any]) -> RawExperiment:
    """
    Extract experiment metadata from JSON content
    """
    raw_experiment = RawExperiment(
        title=json_data["experiment_name"],
        description=json_data["experiment_description"],
        data_classification=None,
        created_by=None,
        url=None,
        locked=False,
        users=None,
        groups=None,
        identifiers=json_data["experiment_ids"],
        projects=[json_data["project"]],
        institution_name=None,
        metadata=None,
        schema=None,
        start_time=None,
        end_time=None,
        created_time=None,
        update_time=None,
        embargo_until=None,
    )

    return raw_experiment


def parse_dataset_info(json_data: dict[str, Any], directory: Path) -> RawDataset:
    """
    Extract dataset metadata from JSON content
    """

    # These will be used separately when we implement the directory-parsing logic
    abi_music_microscope = "abi-music-microscope-v1"
    # abi_music_post_processing = "abi-music-post-processing-v1"

    # These will be used separately when we implement the directory-parsing logic
    dataset_schema_raw = MTUrl("http://abi-music.com/dataset-raw/1")
    #  dataset_schema_zarr = MTUrl("http://abi-music.com/dataset-zarr/1")

    # Currently taking the same fields for Raw and Zarr datasets, but that may change
    metadata: dict[str, Any] = {
        "description": json_data["Description"],
        "sequence-id": json_data["SequenceID"],
        "sqrt-offset": json_data["Offsets"]["SQRT Offset"],
    }

    # N.B. Placeholder. We will get this from dir name for Raw, and dir name prefix for Zarr,
    #      when directory-parsing is implemented
    timestamp = "220228-103800"
    created_time = parse_timestamp(timestamp)

    return RawDataset(
        description=json_data["Description"],
        data_classification=None,
        directory=directory,
        users=None,
        groups=None,
        immutable=False,
        identifiers=[
            json_data["Basename"]["Sequence"],
            str(json_data["SequenceID"]),
        ],
        experiments=[
            json_data["Basename"]["Sample"],
        ],
        instrument=abi_music_microscope,
        metadata=metadata,
        schema=dataset_schema_raw,
        created_time=created_time,
        modified_time=None,
    )


def main() -> None:
    """
    main function - this is just for testing - a proper ingestion runner is yet to be written.
    """

    data_root = Path("/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project")

    project_json_path = data_root / "project.json"
    with project_json_path.open(encoding="utf-8") as f:
        project_data = json.load(f)

    raw_project = parse_project_info(json_data=project_data)
    print("Raw Project:\n", raw_project)

    experiment_json_path = data_root / "sample" / "experiment.json"
    with experiment_json_path.open(encoding="utf-8") as f:
        experiment_data = json.load(f)

    raw_experiment = parse_experiment_info(experiment_data)
    print("Raw Experiment:\n", raw_experiment)

    dataset_json_path = data_root / "sample" / "Ganglia561" / "Ganglia561.json"
    with dataset_json_path.open(encoding="utf-8") as f:
        dataset_data = json.load(f)

    # N.B. have seen a dir in one example JSON, but not all
    # What should this be relative to? Presumably shouldn't be absolute?
    dataset_dir = Path("path/to/dir")

    raw_dataset = parse_dataset_info(dataset_data, dataset_dir)
    print("Raw Dataset:\n", raw_dataset)

    print("Done")


if __name__ == "__main__":
    main()

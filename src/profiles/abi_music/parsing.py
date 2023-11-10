"""
Command-line application for ingesting data from the ABI Music Instrument (Microscope)
"""

import json
from pathlib import Path
from typing import Any

from src.blueprints.common_models import GroupACL
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import ConfigFromEnv
from src.helpers.enumerators import DataClassification


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

    # TODO:
    # - Where do we get the data classification from here
    # - Safe just to assume UoA as institution for now?
    # - What to do for times? Indefinite?
    # - what metadata might we have at the project level? Any? Then schema etc.
    # - where do the users come from? Add Greg? We discussed this but I've forgotten

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
        data_classification=DataClassification.SENSITIVE,  # Get from project?
        created_by=None,
        url=None,
        locked=False,
        users=None,
        groups=None,  # Get from project?
        identifiers=json_data["experiment_ids"],
        projects=[json_data["project"]],
        institution_name=None,
        metadata=None,  # Any metadata for experiments?
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
    return RawDataset(
        description=json_data["description"],
        data_classification=DataClassification.SENSITIVE,
        directory=directory,
        users=None,
        groups=None,
        immutable=False,
        identifiers=[json_data["Basename"]["Sequence"], json_data["SequenceID"]],
        experiments=[json_data["Basename"]["Sample"]],
        instrument="TODO",
        metadata=None,  # TODO
        schema=None,  # TODO
        created_time=None,  # N.B. get this from dir name for Raw. Dir name suffix for Zarr
        modified_time=None,
    )


def main() -> None:
    """
    main function
    """
    config = ConfigFromEnv()

    project_json_path = Path(
        "/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project/project.json"
    )
    with project_json_path.open(encoding="utf-8") as f:
        project_data = json.load(f)

    raw_project = parse_project_info(json_data=project_data)

    experiment_json_path = Path(
        "/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project/sample/experiment.json"
    )
    with experiment_json_path.open(encoding="utf-8") as f:
        experiment_data = json.load(f)

    raw_experiment = parse_experiment_info(experiment_data)

    dataset_json_path = Path(
        "/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project/project.json"
    )
    with dataset_json_path.open(encoding="utf-8") as f:
        dataset_data = json.load(f)

    # N.B. have seen a dir in one example JSON, but not all
    # What should this be relative to? Presumably shouldn't be absolute?
    dataset_dir = Path("path/to/dir")

    raw_dataset = parse_dataset_info(dataset_data, dataset_dir)

    print("Done")


if __name__ == "__main__":
    main()

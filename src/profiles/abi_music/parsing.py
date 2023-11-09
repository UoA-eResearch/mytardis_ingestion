"""
Command-line application for ingesting data from the ABI Music Instrument (Microscope)
"""

import json
from pathlib import Path

from src.blueprints.common_models import GroupACL
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import ConfigFromEnv
from src.helpers.enumerators import DataClassification


def parse_json(config: ConfigFromEnv) -> None:
    project_json_path = Path(
        "/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project/project.json"
    )
    experiment_json_path = Path(
        "/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project/project.json"
    )
    dataset_json_path = Path(
        "/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project/project.json"
    )

    with project_json_path.open(encoding="utf-8") as f:
        project_data = json.load(f)

    with experiment_json_path.open(encoding="utf-8") as f:
        experiment_data = json.load(f)

    with dataset_json_path.open(encoding="utf-8") as f:
        dataset_data = json.load(f)

    groups: list[GroupACL] = []
    for group in project_data["groups"]:
        name = group["name"]
        is_admin = group.get("admin") is True

        if is_admin:
            is_owner, can_download, see_sensitive = True, True, True
        else:
            is_owner = group.get("is_owner") is True
            can_download = group.get("can_download") is True
            see_sensitive = group.get("see_sensitive") is True

        # TODO: do we need to ensure this group exists somehow? Yes, but *I think* referring/sending it in a request causes it to be created?
        #       But do need to go in and add users to the group manually. Or at least one user, who can then be an admin for the group and
        #       add other users.
        group_info = GroupACL(
            group=name,
            is_owner=is_owner,
            can_download=can_download,
            see_sensitive=see_sensitive,
        )
        groups.append(group_info)

    # TODO: what metadata might we have at the project level? Any? Then schema etc.

    # TODO: where do the users come from? Add Greg? We discussed this but I've forgotten

    # TODO:
    # - Where do we get the data classification from here
    # - Safe just to assume UoA as institution for now?
    # - What to do for times? Indefinite?

    raw_project = RawProject(
        name=project_data["project_name"],
        description=project_data["project_description"],
        principal_investigator=project_data["principal_investigator"],
        data_classification=DataClassification.SENSITIVE,
        created_by=project_data["principal_investigator"],
        users=None,
        groups=groups,
        identifiers=project_data["project_ids"],
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


if __name__ == "__main__":
    config = ConfigFromEnv()

    parse_json(config)

    print("Done")

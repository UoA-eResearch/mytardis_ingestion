"""
Parsing logic for generating PEDD dataclasses from ABI Music files
"""

import json
import logging
import mimetypes
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from src.blueprints.common_models import GroupACL
from src.blueprints.custom_data_types import MTUrl
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.helpers.enumerators import DataClassification
from src.utils import log_utils
from src.utils.filesystem import checksums, filters
from src.utils.filesystem.filesystem_nodes import DirectoryNode

# Expected datetime format is "yymmdd-DDMMSS"
datetime_pattern = re.compile("^[0-9]{6}-[0-9]{6}$")

DEFAULT_INSTITUTION = "University of Auckland"


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
        institution=[DEFAULT_INSTITUTION],
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


def parse_dataset_info(json_data: dict[str, Any]) -> tuple[RawDataset, str]:
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

    main_id = json_data["Basename"]["Sequence"]

    dataset = RawDataset(
        description=json_data["Description"],
        data_classification=None,
        directory=None,
        users=None,
        groups=None,
        immutable=False,
        identifiers=[
            main_id,
            str(json_data["SequenceID"]),
        ],
        experiments=[
            json_data["Basename"]["Sample"],
        ],
        instrument=abi_music_microscope,
        metadata=metadata,
        schema=dataset_schema_raw,
        created_time=None,
        modified_time=None,
    )

    return (dataset, main_id)


def collate_datafile_info(
    file_rel_path: Path, root_dir: Path, dataset_name: str
) -> RawDatafile:
    """
    Collect and collate all the information needed to define a datafile dataclass
    """
    full_path = root_dir / file_rel_path

    mimetype, _ = mimetypes.guess_type(full_path)
    if mimetype is None:
        mimetype = "application/octet-stream"

    return RawDatafile(
        filename=file_rel_path.name,
        directory=file_rel_path,
        md5sum=checksums.calculate_md5(full_path),
        mimetype=mimetype,
        size=full_path.stat().st_size,
        users=None,
        groups=None,
        dataset=dataset_name,
        metadata=None,
        schema=None,
    )


def parse_raw_data(
    raw_dir: DirectoryNode, root_dir: Path, file_filter: filters.PathFilterSet
) -> IngestibleDataclasses:
    """
    Parse the directory containing the raw data
    """

    pedd_builder = IngestibleDataclasses()

    project_dirs = [
        d for d in raw_dir.iter_dirs(recursive=True) if d.has_file("project.json")
    ]

    for project_dir in project_dirs:
        print(f"Project directory: {project_dir.name()}")

        project_json = (
            project_dir.file("project.json").path().read_text(encoding="utf-8")
        )
        pedd_builder.add_project(parse_project_info(json.loads(project_json)))

        experiment_dirs = [
            d
            for d in project_dir.iter_dirs(recursive=True)
            if d.has_file("experiment.json")
        ]

        for experiment_dir in experiment_dirs:
            print(f"Experiment directory: {experiment_dir.name()}")

            experiment_json = (
                experiment_dir.file("experiment.json")
                .path()
                .read_text(encoding="utf-8")
            )
            pedd_builder.add_experiment(
                parse_experiment_info(json.loads(experiment_json))
            )

            dataset_dirs = [
                d
                for d in experiment_dir.iter_dirs(recursive=True)
                if d.has_file(d.name() + ".json")
            ]

            for dataset_dir in dataset_dirs:
                print(f"Dataset directory: {dataset_dir.name()}")

                dataset_json = (
                    dataset_dir.file(dataset_dir.name() + ".json")
                    .path()
                    .read_text(encoding="utf-8")
                )

                dataset, dataset_id = parse_dataset_info(json.loads(dataset_json))

                data_dir = next(
                    filter(
                        lambda d: datetime_pattern.match(d.path().stem) is not None,
                        dataset_dir.iter_dirs(),
                    )
                )

                dataset.created_time = parse_timestamp(data_dir.name())

                # TODO: pass the specific instrument ID, schema etc

                pedd_builder.add_dataset(dataset)

                for file in dataset_dir.iter_files(recursive=True):
                    if file_filter.exclude(file.path()):
                        continue

                    file_rel_path = file.path().relative_to(root_dir)

                    datafile = collate_datafile_info(
                        file_rel_path, root_dir, dataset_id
                    )

                    pedd_builder.add_datafile(datafile)

    return pedd_builder


def parse_zarr_data(
    zarr_root: DirectoryNode, root_dir: Path, file_filter: filters.PathFilterSet
) -> IngestibleDataclasses:
    pedd_builder = IngestibleDataclasses()

    for directory in zarr_root.iter_dirs(recursive=True):
        zarr_dirs = directory.find_dirs(
            lambda d: d.name().endswith(".zarr"), recursive=True
        )

        for zarr_dir in zarr_dirs:
            name_stem = zarr_dir.name().removesuffix(".zarr")

            try:
                json_file = directory.file(name_stem + ".json")
            except FileNotFoundError as e:
                raise FileNotFoundError(
                    f"Zarr {zarr_dir.path()} has no corresponding JSON file"
                ) from e

            dataset, dataset_id = parse_dataset_info(
                json.loads(json_file.path().read_text(encoding="utf-8"))
            )

            dataset.created_time = parse_timestamp(name_stem)

            pedd_builder.add_dataset(dataset)

            for file in zarr_dir.iter_files(recursive=True):
                if file_filter.exclude(file.path()):
                    continue

                file_rel_path = file.path().relative_to(root_dir)

                datafile = collate_datafile_info(file_rel_path, root_dir, dataset_id)

                pedd_builder.add_datafile(datafile)

                # TODO: ensure we already have a corresponding project and experiment defined

    # name_format = re.compile(r"(\w+)-(\w+)-(\w+)")

    # TODO: do we search first for ZARR files, then parse the dir name? Or do we even need to parse the dir name? Should Proj/Exp come from JSON file?

    # BenP-PID143-BlockA
    # Project-Experiment-Dataset

    return pedd_builder


# Not sure what it will return yet
def parse_data(root: DirectoryNode) -> None:
    """
    Parse/validate the data directory to extract the files to be ingested
    """

    assert not root.empty(), "Data root directory is empty. May not be mounted."

    raw_dir = root.dir("Vault").dir("Raw")
    zarr_dir = root.dir("Zarr")

    file_filter = filters.PathFilterSet(filter_system_files=True)

    pedd_builder_raw = parse_raw_data(raw_dir, root.path(), file_filter)
    pedd_builder_zarr = parse_zarr_data(zarr_dir, root.path(), file_filter)


def main1() -> None:
    """
    main function - this is just for testing - a proper ingestion runner is yet to be written.
    """

    data_root = Path("/home/andrew/dev/ro-crate-abi-music/JSON templates/Test/project")

    project_json_path = data_root / "project.json"
    with project_json_path.open(encoding="utf-8") as f:
        project_data = json.load(f)

    raw_project = parse_project_info(json_data=project_data)
    print("Raw Project:\n", raw_project.model_dump_json(indent=4))

    experiment_json_path = data_root / "sample" / "experiment.json"
    with experiment_json_path.open(encoding="utf-8") as f:
        experiment_data = json.load(f)

    raw_experiment = parse_experiment_info(experiment_data)
    print("Raw Experiment:\n", raw_experiment.model_dump_json(indent=4))

    dataset_json_path = data_root / "sample" / "Ganglia561" / "Ganglia561.json"
    with dataset_json_path.open(encoding="utf-8") as f:
        dataset_data = json.load(f)

    raw_dataset, _ = parse_dataset_info(dataset_data)
    print("Raw Dataset:\n", raw_dataset.model_dump_json(indent=4))

    print("Done")


def main2() -> None:
    log_utils.init_logging(file_name="abi_ingest.log", level=logging.DEBUG)

    # Should come from command-line args or config file
    data_root = Path("/mnt/abi_test_data")

    root_node = DirectoryNode(data_root)

    parse_data(root_node)

    print("Done")


if __name__ == "__main__":
    # main1()
    main2()

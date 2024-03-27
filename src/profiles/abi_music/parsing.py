"""
Parsing logic for generating PEDD dataclasses from ABI Music files
"""

import json
import logging
import mimetypes
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from src.blueprints.common_models import GroupACL, UserACL
from src.blueprints.custom_data_types import MTUrl
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.extraction.manifest import IngestionManifest
from src.extraction.metadata_extractor import IMetadataExtractor
from src.mytardis_client.enumerators import DataClassification
from src.profiles.abi_music.abi_music_consts import (
    ABI_MUSIC_DATASET_RAW_SCHEMA,
    ABI_MUSIC_DATASET_ZARR_SCHEMA,
    ABI_MUSIC_MICROSCOPE_INSTRUMENT,
    ABI_MUSIC_POSTPROCESSING_INSTRUMENT,
    DEFAULT_INSTITUTION,
)
from src.utils.filesystem import checksums, filters
from src.utils.filesystem.filesystem_nodes import DirectoryNode, FileNode

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


def read_json(file: FileNode) -> dict[str, Any]:
    """Extract the JSON data hierachy from `file`"""
    file_data = file.path().read_text(encoding="utf-8")
    json_data: dict[str, Any] = json.loads(file_data)
    return json_data


def parse_project_info(directory: DirectoryNode) -> RawProject:
    """
    Extract project metadata from JSON content
    """

    json_data = read_json(directory.file("project.json"))

    def read_permissions(json_node: Mapping[str, Any]) -> dict[str, bool]:
        is_admin: bool = json_node.get("admin") is True

        return {
            "is_owner": is_admin or json_node.get("is_owner") is True,
            "can_download": is_admin or json_node.get("can_download") is True,
            "see_sensitive": is_admin or json_node.get("see_sensitive") is True,
        }

    groups: list[GroupACL] = []
    for group_node in json_data.get("groups", []):
        permissions = read_permissions(group_node)
        groups.append(GroupACL(group=group_node["name"], **permissions))

    users: list[UserACL] = []
    for user_node in json_data.get("users", []):
        permissions = read_permissions(user_node)
        users.append(UserACL(user=user_node["user"], **permissions))

    raw_project = RawProject(
        name=json_data["project_name"],
        description=json_data["project_description"],
        principal_investigator=json_data["principal_investigator"],
        data_classification=DataClassification.SENSITIVE,
        created_by=json_data["principal_investigator"],
        users=users or None,
        groups=groups or None,
        identifiers=json_data["project_ids"],
        institution=[DEFAULT_INSTITUTION],
        metadata=None,
        schema=None,
        start_time=None,
        end_time=None,
        embargo_until=None,
        delete_in_days=-1,
        archive_in_days=365,
    )

    return raw_project


def parse_experiment_info(directory: DirectoryNode) -> RawExperiment:
    """
    Extract experiment metadata from JSON content
    """

    json_data = read_json(directory.file("experiment.json"))

    # The data features both "project" and "projects" keys, so we need to handle both
    projects: list[str] = json_data.get("projects") or [json_data["project"]]

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
        projects=projects,
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


def parse_raw_dataset(directory: DirectoryNode) -> tuple[RawDataset, str]:
    """
    Extract Raw dataset metadata from JSON content
    """

    json_data = read_json(directory.file(directory.name() + ".json"))

    metadata: dict[str, Any] = {
        "full-description": json_data["Description"],
        "sqrt-offset": json_data["Offsets"]["SQRT Offset"],
    }

    sequence_name = json_data["Basename"]["Sequence"]
    main_id = sequence_name + "-raw"

    dataset = RawDataset(
        description=sequence_name + ":raw",
        data_classification=None,
        directory=None,
        users=None,
        groups=None,
        immutable=False,
        identifiers=[
            main_id,
            str(json_data["SequenceID"]) + "-raw",
        ],
        experiments=[
            json_data["Basename"]["Sample"],
        ],
        instrument=ABI_MUSIC_MICROSCOPE_INSTRUMENT,
        metadata=metadata,
        schema=MTUrl(ABI_MUSIC_DATASET_RAW_SCHEMA),
        created_time=None,
        modified_time=None,
    )

    return (dataset, main_id)


def parse_zarr_dataset(directory: DirectoryNode) -> tuple[RawDataset, str]:
    """
    Extract Zarr dataset metadata from JSON content
    """

    try:
        json_file_name = directory.name().replace(".zarr", ".json")
        json_file = directory.parent().file(json_file_name)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Zarr {directory.path()} has no corresponding JSON file"
        ) from e

    json_data = read_json(json_file)

    metadata: dict[str, Any] = {
        "full-description": json_data["config"]["Description"],
        "sqrt-offset": json_data["config"]["Offsets"]["SQRT Offset"],
    }

    sequence_name = json_data["config"]["Basename"]["Sequence"]

    main_id = sequence_name + "-zarr"

    dataset = RawDataset(
        description=sequence_name + ":zarr",
        data_classification=None,
        directory=None,
        users=None,
        groups=None,
        immutable=False,
        identifiers=[
            main_id,
            str(json_data["config"]["SequenceID"]) + "-zarr",
        ],
        experiments=[
            json_data["config"]["Basename"]["Sample"],
        ],
        instrument=ABI_MUSIC_POSTPROCESSING_INSTRUMENT,
        metadata=metadata,
        schema=ABI_MUSIC_DATASET_ZARR_SCHEMA,
        created_time=None,
        modified_time=None,
    )

    return (dataset, main_id)


def collate_datafile_info(
    file: FileNode, root_dir: Path, dataset_name: str
) -> RawDatafile:
    """
    Collect and collate all the information needed to define a datafile dataclass
    """
    file_rel_path = file.path().relative_to(root_dir)

    mimetype, _ = mimetypes.guess_type(file.name())
    if mimetype is None:
        mimetype = "application/octet-stream"

    return RawDatafile(
        filename=file_rel_path.name,
        directory=file_rel_path,
        md5sum=checksums.calculate_md5(file.path()),
        mimetype=mimetype,
        size=file.stat().st_size,
        users=None,
        groups=None,
        dataset=dataset_name,
        metadata=None,
        schema=None,
    )


def parse_raw_data(
    root: DirectoryNode, file_filter: filters.PathFilterSet
) -> IngestionManifest:
    """
    Parse the directory containing the raw data
    """

    manifest = IngestionManifest(source_data_root=root.path())

    project_dirs = [
        d for d in root.iter_dirs(recursive=True) if d.has_file("project.json")
    ]

    for project_dir in project_dirs:
        logging.info("Project directory: %s", project_dir.name())

        manifest.add_project(parse_project_info(project_dir))

        experiment_dirs = [
            d
            for d in project_dir.iter_dirs(recursive=True)
            if d.has_file("experiment.json")
        ]

        for experiment_dir in experiment_dirs:
            logging.info("Experiment directory: %s", experiment_dir.name())

            manifest.add_experiment(parse_experiment_info(experiment_dir))

            dataset_dirs = [
                d
                for d in experiment_dir.iter_dirs(recursive=True)
                if d.has_file(d.name() + ".json")
            ]

            for dataset_dir in dataset_dirs:
                logging.info("Dataset directory: %s", dataset_dir.name())

                dataset, dataset_id = parse_raw_dataset(dataset_dir)

                data_dir = next(
                    d
                    for d in dataset_dir.iter_dirs()
                    if datetime_pattern.match(d.path().stem)
                )

                dataset.created_time = parse_timestamp(data_dir.name())

                manifest.add_dataset(dataset)

                for file in dataset_dir.iter_files(recursive=True):
                    if file_filter.exclude(file.path()):
                        continue

                    datafile = collate_datafile_info(file, root.path(), dataset_id)
                    manifest.add_datafile(datafile)

    return manifest


def parse_zarr_data(
    root: DirectoryNode, file_filter: filters.PathFilterSet
) -> IngestionManifest:
    """
    Parse the directory containing the derived/post-processed Zarr data
    """
    manifest = IngestionManifest(source_data_root=root.path())

    for directory in root.iter_dirs(recursive=True):
        zarr_dirs = directory.find_dirs(
            lambda d: d.name().endswith(".zarr"), recursive=True
        )

        for zarr_dir in zarr_dirs:
            dataset, dataset_id = parse_zarr_dataset(zarr_dir)

            name_stem = zarr_dir.name().removesuffix(".zarr")
            dataset.created_time = parse_timestamp(name_stem)

            # Note: the parent directory name is expected to be in the format
            # <Project>-<Experiment>-<Dataset>. Should we cross-check against these?

            manifest.add_dataset(dataset)

            for file in zarr_dir.iter_files(recursive=True):
                if file_filter.exclude(file.path()):
                    continue

                datafile = collate_datafile_info(file, root.path(), dataset_id)

                manifest.add_datafile(datafile)

    return manifest


def link_zarr_to_raw(
    zarr_datasets: list[RawDataset], raw_datasets: list[RawDataset]
) -> None:
    """Augment Zarr dataset metadata with information about the source dataset.

    Mutates `zarr_datasets` in-place; should it?
    """
    for zarr_dataset in zarr_datasets:
        # Note: not ideal to repeat the prefixing logic here - should we do
        # this linking at an earlier stage?
        raw_dataset = next(
            ds
            for ds in raw_datasets
            if ds.description == zarr_dataset.description.replace(":zarr", ":raw")
        )
        zarr_dataset.metadata = zarr_dataset.metadata or {}
        zarr_dataset.metadata["raw_dataset"] = raw_dataset.description


def parse_data(root: DirectoryNode) -> IngestionManifest:
    """
    Parse/validate the data directory to extract the files to be ingested
    """

    file_filter = filters.PathFilterSet(filter_system_files=True)

    dc_raw = parse_raw_data(root, file_filter)
    dc_zarr = parse_zarr_data(root, file_filter)

    link_zarr_to_raw(dc_zarr.get_datasets(), dc_raw.get_datasets())

    # Note: maybe we should just directly append to the object inside the parsing functions
    return IngestionManifest(
        source_data_root=root.path(),
        projects=dc_raw.get_projects() + dc_zarr.get_projects(),
        experiments=dc_raw.get_experiments() + dc_zarr.get_experiments(),
        datasets=dc_raw.get_datasets() + dc_zarr.get_datasets(),
        datafiles=dc_raw.get_datafiles() + dc_zarr.get_datafiles(),
    )


class ABIMusicExtractor(IMetadataExtractor):
    """Metadata extractor for the ABI MuSIC data"""

    def __init__(self) -> None:
        pass

    def extract(self, root_dir: Path) -> IngestionManifest:
        root = DirectoryNode(root_dir)
        return parse_data(root)

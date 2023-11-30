"""
Parsing logic for generating PEDD dataclasses from ABI Music files
"""

import io
import json
import logging
import mimetypes
import re
import sys
import time
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
from src.profiles.abi_music.abi_music_consts import (
    ABI_MUSIC_DATASET_RAW_SCHEMA,
    ABI_MUSIC_DATASET_ZARR_SCHEMA,
    ABI_MUSIC_MICROSCOPE_INSTRUMENT,
    ABI_MUSIC_POSTPROCESSING_INSTRUMENT,
    DEFAULT_INSTITUTION,
)
from src.utils import log_utils
from src.utils.filesystem import checksums, filters
from src.utils.filesystem.filesystem_nodes import DirectoryNode, FileNode

# Expected datetime format is "yymmdd-DDMMSS"
datetime_pattern = re.compile("^[0-9]{6}-[0-9]{6}$")

# TODO: these should not be committed in this form - just use MD5 cache to speed
# up development
cache_path = Path("/home/andrew/dev/mti_1/.ids_cache")
cache_path.mkdir(exist_ok=True)


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


def calculate_md5(data_root: Path, path: Path) -> str:
    """Calculate MD5 checksum or retrieve from cache

    Just used to speed up development, as computing hashes
    takes a long time when retrieving files over network.
    """

    cache_dir = DirectoryNode(cache_path)

    rel_path = path.relative_to(data_root)
    cached_value_path = Path(str(cache_dir.path() / rel_path) + ".md5")

    if cached_value_path.is_file():
        md5 = cached_value_path.read_text(encoding="utf-8")
    else:
        md5 = checksums.calculate_md5(path)
        cached_value_path.parent.mkdir(exist_ok=True, parents=True)
        cached_value_path.write_text(md5, encoding="utf-8")

    return md5


def parse_project_info(directory: DirectoryNode) -> RawProject:
    """
    Extract project metadata from JSON content
    """

    json_data = read_json(directory.file("project.json"))

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


def parse_experiment_info(directory: DirectoryNode) -> RawExperiment:
    """
    Extract experiment metadata from JSON content
    """

    json_data = read_json(directory.file("experiment.json"))

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


def parse_raw_dataset(directory: DirectoryNode) -> tuple[RawDataset, str]:
    """
    Extract Raw dataset metadata from JSON content
    """

    json_data = read_json(directory.file(directory.name() + ".json"))

    metadata: dict[str, Any] = {
        "description": json_data["Description"],
        "sequence-id": json_data["SequenceID"],
        "sqrt-offset": json_data["Offsets"]["SQRT Offset"],
    }

    main_id = json_data["Basename"]["Sequence"]

    dataset = RawDataset(
        description=json_data["Description"] + "_raw",
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
        "description": json_data["config"]["Description"],
        "sequence-id": json_data["config"]["SequenceID"],
        "sqrt-offset": json_data["config"]["Offsets"]["SQRT Offset"],
    }

    main_id = json_data["config"]["Basename"]["Sequence"]

    dataset = RawDataset(
        description=json_data["config"]["Description"] + "_zarr",
        data_classification=None,
        directory=None,
        users=None,
        groups=None,
        immutable=False,
        identifiers=[
            main_id,
            str(json_data["config"]["SequenceID"]),
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
        # md5sum=checksums.calculate_md5(file.path()),
        md5sum=calculate_md5(root_dir, file.path()),
        mimetype=mimetype,
        size=file.stat().st_size,
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

        pedd_builder.add_project(parse_project_info(project_dir))

        experiment_dirs = [
            d
            for d in project_dir.iter_dirs(recursive=True)
            if d.has_file("experiment.json")
        ]

        for experiment_dir in experiment_dirs:
            print(f"Experiment directory: {experiment_dir.name()}")

            pedd_builder.add_experiment(parse_experiment_info(experiment_dir))

            dataset_dirs = [
                d
                for d in experiment_dir.iter_dirs(recursive=True)
                if d.has_file(d.name() + ".json")
            ]

            for dataset_dir in dataset_dirs:
                print(f"Dataset directory: {dataset_dir.name()}")

                dataset, dataset_id = parse_raw_dataset(dataset_dir)

                data_dir = next(
                    d
                    for d in dataset_dir.iter_dirs()
                    if datetime_pattern.match(d.path().stem)
                )

                dataset.created_time = parse_timestamp(data_dir.name())

                pedd_builder.add_dataset(dataset)

                for file in dataset_dir.iter_files(recursive=True):
                    if file_filter.exclude(file.path()):
                        continue

                    datafile = collate_datafile_info(file, root_dir, dataset_id)
                    pedd_builder.add_datafile(datafile)

    return pedd_builder


def parse_zarr_data(
    zarr_root: DirectoryNode, root_dir: Path, file_filter: filters.PathFilterSet
) -> IngestibleDataclasses:
    """
    Parse the directory containing the derived/post-processed Zarr data
    """
    pedd_builder = IngestibleDataclasses()

    for directory in zarr_root.iter_dirs(recursive=True):
        zarr_dirs = directory.find_dirs(
            lambda d: d.name().endswith(".zarr"), recursive=True
        )

        for zarr_dir in zarr_dirs:
            dataset, dataset_id = parse_zarr_dataset(zarr_dir)

            name_stem = zarr_dir.name().removesuffix(".zarr")
            dataset.created_time = parse_timestamp(name_stem)

            # Note: the parent directory name is expected to be in the format
            # <Project>-<Experiment>-<Dataset>. Should we cross-validate against these?

            pedd_builder.add_dataset(dataset)

            for file in zarr_dir.iter_files(recursive=True):
                if file_filter.exclude(file.path()):
                    continue

                datafile = collate_datafile_info(file, root_dir, dataset_id)

                pedd_builder.add_datafile(datafile)

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

    dc_raw = parse_raw_data(raw_dir, root.path(), file_filter)
    dc_zarr = parse_zarr_data(zarr_dir, root.path(), file_filter)

    # Add name of raw dataset as metadata for Zarr dataset
    for zarr_dataset in dc_zarr.get_datasets():
        raw_dataset = next(
            ds
            for ds in dc_raw.get_datasets()
            if ds.description == zarr_dataset.description.replace("_zarr", "_raw")
        )
        zarr_dataset.metadata = zarr_dataset.metadata or {}
        zarr_dataset.metadata["raw_dataset"] = raw_dataset.description

    dataclasses = IngestibleDataclasses.merge(dc_raw, dc_zarr)

    # dataclasses.print(sys.stdout)

    stream = io.StringIO()
    dataclasses.print(stream)
    logging.info(stream.getvalue())


def main() -> None:
    """
    main function - this is just for testing - a proper ingestion runner is yet to be written.
    """
    log_utils.init_logging(file_name="abi_ingest.log", level=logging.DEBUG)

    # Should come from command-line args or config file
    data_root = Path("/mnt/abi_test_data")

    root_node = DirectoryNode(data_root)

    start = time.perf_counter(), time.process_time()

    parse_data(root_node)

    end = time.perf_counter(), time.process_time()
    print(f"Time:\n{end[0] - start[0]}\n{end[1] - start[1]}")


if __name__ == "__main__":
    main()

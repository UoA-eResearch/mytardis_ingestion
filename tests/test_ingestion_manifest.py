# pylint: disable=missing-docstring
# mypy: disable-error-code="no-untyped-def"

import json
from datetime import datetime
from pathlib import Path

import pytest

from src.blueprints.common_models import GroupACL, UserACL
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.extraction.manifest import IngestionManifest
from src.mytardis_client.common_types import DataClassification
from src.utils.filesystem.filesystem_nodes import DirectoryNode

# pylint: disable=line-too-long
# NOTE: pyfakefs's FakeFilesystem does not currently work for these tests, because
#       of an interaction with pathlib. This should apparently be fixed with changes
#       to pathlib in Python 3.11.
#       https://pytest-pyfakefs.readthedocs.io/en/latest/troubleshooting.html#pathlib-path-objects-created-outside-of-tests


@pytest.fixture(name="ingestion_manifest")
def _() -> IngestionManifest:
    raw_projects = [
        RawProject(
            name="project1_name",
            description="project1_description",
            principal_investigator="aupi001",
            institution=["institution_name"],
            data_classification=DataClassification.SENSITIVE,
            created_by="aupi001",
            url=None,
            users=[UserACL(user="aupi003"), UserACL(user="aupi004")],
            groups=[GroupACL(group="group_1")],
            identifiers=["project2_id"],
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/project_schema",
            start_time=datetime(2024, 1, 1, 12, 41, 36),
            end_time=datetime(2025, 1, 1, 12, 41, 36),
            embargo_until=datetime(2024, 6, 1, 12, 41, 36),
        ),
        RawProject(
            name="project2_name",
            description="project2_description",
            principal_investigator="aupi001",
            institution=["institution_name"],
            data_classification=DataClassification.PUBLIC,
            created_by="aupi002",
            url=None,
            users=[UserACL(user="aupi004"), UserACL(user="aupi005")],
            groups=[GroupACL(group="group_1")],
            identifiers=["project2_id"],
            metadata={
                "field_1": "value_1",
            },
            schema="https://example.com/project_schema",
            start_time=datetime(2023, 1, 1, 12, 41, 36),
            end_time=datetime(2024, 1, 1, 12, 41, 36),
            embargo_until=datetime(2023, 6, 1, 12, 41, 36),
        ),
    ]
    raw_experiments = [
        RawExperiment(
            title="experiment1_title",
            description="experiment1_description",
            data_classification=DataClassification.SENSITIVE,
            created_by="aupi001",
            locked=True,
            users=[UserACL(user="aupi003"), UserACL(user="aupi004")],
            groups=[GroupACL(group="group_1")],
            identifiers=["experiment1_id"],
            projects=["project1_name"],
            institution_name="institution_name",
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/experiment_schema",
            start_time=datetime(2024, 1, 1, 12, 41, 36),
            end_time=datetime(2025, 1, 1, 12, 41, 36),
            created_time=datetime(2024, 1, 1, 12, 41, 36),
            update_time=datetime(2024, 1, 1, 12, 41, 36),
            embargo_until=datetime(2024, 6, 1, 12, 41, 36),
        ),
        RawExperiment(
            title="experiment2_title",
            description="experiment2_description",
            data_classification=DataClassification.PUBLIC,
            created_by="aupi002",
            locked=False,
            users=[UserACL(user="aupi004"), UserACL(user="aupi005")],
            groups=[GroupACL(group="group_1")],
            identifiers=["experiment2_id"],
            projects=["project2_name"],
            institution_name="institution_name",
            metadata={
                "field_1": "value_1",
            },
            schema="https://example.com/experiment_schema",
            start_time=datetime(2023, 1, 1, 12, 41, 36),
            end_time=datetime(2024, 1, 1, 12, 41, 36),
            created_time=datetime(2023, 1, 1, 12, 41, 36),
            update_time=datetime(2023, 1, 1, 12, 41, 36),
            embargo_until=datetime(2023, 6, 1, 12, 41, 36),
        ),
    ]
    raw_datasets = [
        RawDataset(
            description="dataset1_description",
            data_classification=DataClassification.PUBLIC,
            directory=Path("path/to/dataset/1"),
            users=[UserACL(user="aupi003"), UserACL(user="aupi004")],
            groups=[GroupACL(group="group_1")],
            immutable=True,
            identifiers=["dataset1_id"],
            experiments=["experiment1_title"],
            instrument="instrument_name",
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/dataset_schema",
            created_time=datetime(2024, 1, 1, 12, 41, 36),
            modified_time=datetime(2024, 1, 1, 12, 48, 36),
        ),
        RawDataset(
            description="dataset2_description",
            data_classification=DataClassification.SENSITIVE,
            directory=Path("path/to/dataset/2"),
            users=[UserACL(user="aupi005"), UserACL(user="aupi006")],
            groups=[GroupACL(group="group_2")],
            immutable=False,
            identifiers=["dataset2_id"],
            experiments=["experiment2_title"],
            instrument="instrument_name",
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/dataset_schema",
            created_time=datetime(2023, 1, 1, 12, 41, 36),
            modified_time=datetime(2023, 1, 1, 12, 48, 36),
        ),
        RawDataset(
            description="dataset3_description",
            data_classification=DataClassification.PUBLIC,
            directory=Path("path/to/dataset/3"),
            users=[UserACL(user="aupi007"), UserACL(user="aupi008")],
            groups=[GroupACL(group="group_2")],
            immutable=False,
            identifiers=["dataset3_id"],
            experiments=["experiment3_title"],
            instrument="instrument_name",
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/dataset_schema",
            created_time=datetime(2022, 1, 1, 12, 41, 36),
            modified_time=datetime(2022, 1, 1, 12, 48, 36),
        ),
    ]
    raw_datafiles = [
        RawDatafile(
            filename="file_1.txt",
            directory=Path("path/to/file_dir/1"),
            md5sum="d41d8cd98f00b204e9800998ecf8427e",
            mimetype="text/plain",
            size=100,
            users=[UserACL(user="aupi003")],
            groups=[GroupACL(group="group_1")],
            dataset="dataset1_description",
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/datafile_schema",
        ),
        RawDatafile(
            filename="file_2.txt",
            directory=Path("path/to/file_dir/2"),
            md5sum="gfji3cd98f00b204e9800998ecf8427e",
            mimetype="text/plain",
            size=1000,
            users=[UserACL(user="aupi005")],
            groups=[GroupACL(group="group_2")],
            dataset="dataset2_description",
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/datafile_schema",
        ),
        RawDatafile(
            filename="file_3.txt",
            directory=Path("path/to/file_dir/3"),
            md5sum="d41d8cd98f00b204e9867998ecf8427e",
            mimetype="text/plain",
            size=45654,
            users=[UserACL(user="aupi007")],
            groups=[GroupACL(group="group_2")],
            dataset="dataset3_description",
            metadata={
                "field_1": "value_1",
                "field_2": "value_2",
            },
            schema="https://example.com/datafile_schema",
        ),
        RawDatafile(
            filename="file_4.txt",
            directory=Path("path/to/file_dir/4"),
            md5sum="d41d8cd98f00b204e9800998ecf8427e",
            mimetype="text/plain",
            size=3450,
            users=[UserACL(user="aupi009")],
            groups=[GroupACL(group="group_1")],
            dataset="dataset4_description",
        ),
        RawDatafile(
            filename="file_5.txt",
            directory=Path("path/to/file_dir/5"),
            md5sum="d41d8cd98f00b204e9800998ecf8427e",
            mimetype="text/plain",
            size=10000,
            users=[UserACL(user="aupi010")],
            groups=[GroupACL(group="group_2")],
            dataset="dataset5_description",
        ),
    ]

    manifest = IngestionManifest(
        source_data_root=Path("/data/root/dir"),
        projects=raw_projects,
        experiments=raw_experiments,
        datasets=raw_datasets,
        datafiles=raw_datafiles,
    )
    return manifest


def test_deserialize_invalid_dir() -> None:
    with pytest.raises(NotADirectoryError):
        IngestionManifest.deserialize(Path("does/not/exist"))


def test_serialize(ingestion_manifest: IngestionManifest, tmp_path: Path) -> None:
    output_dir = tmp_path / "test_serialize"
    output_dir.mkdir()

    ingestion_manifest.serialize(output_dir)

    source_info_file = output_dir / "source.json"
    assert source_info_file.is_file(), "Source info file should be created"

    source_info = json.loads(source_info_file.read_text(encoding="utf-8"))
    assert source_info["source_data_root"] == "/data/root/dir"

    projects_dir = DirectoryNode(output_dir / "projects")
    experiments_dir = DirectoryNode(output_dir / "experiments")
    datasets_dir = DirectoryNode(output_dir / "datasets")
    datafiles_dir = DirectoryNode(output_dir / "datafiles")

    assert projects_dir.path().is_dir(), "Projects directory should be created"
    assert experiments_dir.path().is_dir(), "Experiments directory should be created"
    assert datasets_dir.path().is_dir(), "Datasets directory should be created"
    assert datafiles_dir.path().is_dir(), "Datafiles directory should be created"

    assert len(projects_dir.files()) == 2
    assert len(experiments_dir.files()) == 2
    assert len(datasets_dir.files()) == 3
    assert len(datafiles_dir.files()) == 5

    # Just check we get some valid JSON files
    for file in projects_dir.files():
        _ = json.loads(file.path().read_text())

    for file in experiments_dir.files():
        _ = json.loads(file.path().read_text())

    for file in datasets_dir.files():
        _ = json.loads(file.path().read_text())

    for file in datafiles_dir.files():
        _ = json.loads(file.path().read_text())


def test_serialize_deserialize_cycle(
    ingestion_manifest: IngestionManifest, tmp_path: Path
) -> None:
    directory = tmp_path / "test_roundtrip"
    directory.mkdir()

    ingestion_manifest.serialize(directory)

    reloaded_manifest = IngestionManifest.deserialize(directory)

    assert ingestion_manifest.get_data_root() == reloaded_manifest.get_data_root()
    assert ingestion_manifest.get_projects() == reloaded_manifest.get_projects()
    assert ingestion_manifest.get_experiments() == reloaded_manifest.get_experiments()
    assert ingestion_manifest.get_datasets() == reloaded_manifest.get_datasets()
    assert ingestion_manifest.get_datafiles() == reloaded_manifest.get_datafiles()

# pylint: disable=missing-function-docstring
# nosec assert_used
import copy
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.blueprints.common_models import GroupACL, UserACL
from src.blueprints.custom_data_types import Username
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.profiles.ro_crate.ro_crate_parser import ROCrateParser


@pytest.fixture()
def fixture_rocrate_uuid() -> str:
    return uuid.uuid4().hex


@pytest.fixture()
def fixture_ro_crate_name() -> str:
    return "Testing_Ro-crate"


@pytest.fixture()
def fixture_rocrate_project(raw_project: RawProject) -> RawProject:
    ro_crate_project = copy.copy(raw_project)
    return ro_crate_project


@pytest.fixture()
def fixture_rocrate_experiment(raw_experiment: RawExperiment) -> RawExperiment:
    ro_crate_experiment = copy.copy(raw_experiment)
    return ro_crate_experiment


@pytest.fixture()
def fixture_rocrate_dataset(
    raw_dataset: RawDataset, ro_crate_dataset_dir: Path
) -> RawDataset:
    ro_crate_dataset = copy.copy(raw_dataset)
    ro_crate_dataset.description = ro_crate_dataset_dir.as_posix()
    return ro_crate_dataset


@pytest.fixture()
def fixture_rocrate_datafile(
    raw_datafile: RawDatafile, ro_crate_dataset_dir: Path
) -> RawDatafile:
    ro_crate_datafile = copy.copy(raw_datafile)
    ro_crate_datafile.filename = (
        ro_crate_dataset_dir / raw_datafile.filename
    ).as_posix()
    return ro_crate_datafile


@pytest.fixture(name="fakecrate_root")
def fakecrate_root() -> Path:
    return Path("fake_ro_crate")


@pytest.fixture()
def ro_crate_dataset_dir(dataset_dir: Path) -> Path:
    return dataset_dir


@pytest.fixture()
def ro_crate_unlisted_file_dir(ro_crate_dataset_dir: Path) -> Path:
    return ro_crate_dataset_dir / "unlisted_files_dir"


# TODO replace with MyTardis to RO-Crate serializer when that exists
@pytest.fixture()
def test_rocrate_content(
    ro_crate_dataset_dir: Path,
    fixture_rocrate_uuid: str,
    fixture_ro_crate_name: str,
    fixture_rocrate_project: RawProject,
    fixture_rocrate_experiment: RawExperiment,
    fixture_rocrate_dataset: RawDataset,
    fixture_rocrate_datafile: RawDatafile,
) -> str:
    return json.dumps(
        {
            "@context": "https://w3id.org/ro/crate/1.1/context",
            "@graph": [
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "hasPart": [
                        {"@id": ro_crate_dataset_dir.as_posix()},
                        {"@id": fixture_rocrate_datafile.filename},
                    ],
                    "includedInDataCatalog": fixture_rocrate_experiment.title,
                    "instrument": "dummy-ro-crate-meta",
                    "identifier": [
                        {
                            "@id": "Crate_UUID",
                            "@type": "PropertyValue",
                            "name": "RO-CrateUUID",
                            "value": fixture_rocrate_uuid,
                        },
                        {
                            "@id": "Crate_Name",
                            "@type": "PropertyValue",
                            "name": "RO-CrateName",
                            "value": fixture_ro_crate_name,
                        },
                    ],
                    "metadata": ["#test-ro-crate-Metadata"],
                },
                {
                    "@id": fixture_rocrate_dataset.description,
                    "@type": "Dataset",
                    # "description": raw_dataset.description,
                    "includedInDataCatalog": fixture_rocrate_dataset.experiments,
                    "instrument": fixture_rocrate_dataset.instrument,
                    "name": fixture_rocrate_dataset.description,
                    "hasPart": [{"@id": (fixture_rocrate_datafile.filename)}],
                },
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "about": {"@id": "./"},
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                },
                {
                    "@id": fixture_rocrate_project.name,
                    "@type": "Project",
                    "name": fixture_rocrate_project.name,
                    "founder": fixture_rocrate_project.principal_investigator,
                    "url": fixture_rocrate_project.url,
                    "description": fixture_rocrate_project.description,
                },
                {
                    "@id": fixture_rocrate_experiment.title,
                    "@type": "DataCatalog",
                    "name": fixture_rocrate_experiment.title,
                    "project": fixture_rocrate_experiment.projects,
                    "description": fixture_rocrate_experiment.description,
                },
                {
                    "@id": fixture_rocrate_datafile.filename,
                    "@type": ["File"],
                    "name": fixture_rocrate_datafile.filename,
                },
                {
                    "@id": "#test-ro-crate-Metadata",
                    "@type": "MyTardis-Metadata_field",
                    "name": "test-ro-crate-Metadata",
                    "value": "test value",
                    "mt-type": "string",
                    "sensitive": "False",
                },
            ],
        }
    )


@pytest.fixture(name="rocrate_profile_json")
def rocrate_profile_json() -> str:
    return json.dumps(
        {
            "project": {
                "@id": "name",
                "founder": "principal_investigator",
            },
            "experiment": {
                "@id": "title",
                "project": "projects",
            },
            "dataset": {
                "@id": "directory",
                "name": "description",
                "includedInDataCatalog": "experiments",
                "instrument": "instrument",
            },
            "datafile": {
                "md5sum": "md5sum",
                "encodingFormat": "mimetype",
                "contentSize": "size",
                "name": "crate_name",
            },
        }
    )


@pytest.fixture(name="fixture_fake_ro_crate")
def fixture_fake_ro_crate(
    tmp_path: Path,
    test_rocrate_content: str,
    fakecrate_root: Path,
    rocrate_profile_json: str,
    raw_datafile: RawDatafile,
    ro_crate_dataset_dir: Path,
    ro_crate_unlisted_file_dir: Path,
) -> Path:
    crate_root = tmp_path / fakecrate_root / "data/"
    crate_root.mkdir(parents=True, exist_ok=True)
    with open(crate_root / "ro-crate-metadata.json", "w", encoding="utf-8") as f:
        f.write(test_rocrate_content)

    dataset_path = crate_root / ro_crate_dataset_dir
    dataset_path.mkdir(parents=True, exist_ok=True)
    with open(dataset_path / raw_datafile.filename, "w", encoding="utf-8") as f:
        f.write("size > 0")
    dataset_path = crate_root / ro_crate_unlisted_file_dir
    dataset_path.mkdir(parents=True, exist_ok=True)
    with open(dataset_path / "unlisted_file.txt", "w", encoding="utf-8") as f:
        f.write("size > 0")
    return crate_root


@pytest.fixture()
def fixture_rocrate_parser(
    fixture_fake_ro_crate: Path, fakecrate_root: Path
) -> ROCrateParser:
    return ROCrateParser(fixture_fake_ro_crate)

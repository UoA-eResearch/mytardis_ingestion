# pylint: disable=missing-function-docstring
# nosec assert_used
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from rocrate.rocrate import DataEntity

import src.utils.filesystem.filters as filters
from src.blueprints.common_models import GroupACL, Parameter, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime, Username
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.profiles.ro_crate._consts import CRATE_TO_TARDIS_PROFILE
from src.profiles.ro_crate.ro_crate_parser import ROCrateParser


@pytest.fixture()
def fixture_rocrate_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture()
def fixture_ingested_rocrate_project(
    project_name: str,
    project_description: str,
    project_ids: List[str],
    project_principal_investigator: str,
    project_institutions: List[str],
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    created_by_upi: str,
    project_metadata: Dict[str, Any],
    project_url: str,
    start_time_datetime: datetime,
    end_time_datetime: datetime,
) -> RawProject:
    return RawProject(
        name=project_name,
        description=project_description,
        principal_investigator=Username(project_principal_investigator),
        url=project_url,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        institution=project_institutions,
        created_by=created_by_upi,
        identifiers=project_ids,
        metadata=project_metadata,
        start_time=start_time_datetime,
        end_time=end_time_datetime,
    )


# fixture_ingested_rocrate_experiment

# fixture_ingested_rocrate_dataset

# fixture_ingested_rocrate_datafile

# fixture_ingested_rocrate_dataondisk


@pytest.fixture()
def ro_crate_dataset_dir(raw_dataset: RawDataset) -> Path:
    return Path(raw_dataset.directory)


@pytest.fixture()
def test_rocrate_content(
    raw_project: RawProject,
    raw_dataset: RawDataset,
    raw_experiment: RawExperiment,
    raw_datafile: RawDatafile,
    ro_crate_dataset_dir: Path,
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
                        {
                            "@id": (
                                ro_crate_dataset_dir / raw_datafile.filename
                            ).as_posix()
                        },
                    ],
                    "includedInDataCatalog": raw_experiment.title,
                    "instrument": "dummy-ro-crate-meta",
                    "identifier": [
                        {
                            "@id": "Crate_UUID",
                            "@type": "PropertyValue",
                            "name": "RO-CrateUUID",
                            "value": "",
                        },
                        {
                            "@id": "Crate_Name",
                            "@type": "PropertyValue",
                            "name": "RO-CrateName",
                            "value": "Testing_Ro-crate",
                        },
                    ],
                },
                {
                    "@id": ro_crate_dataset_dir.as_posix(),
                    "@type": "Dataset",
                    # "description": raw_dataset.description,
                    "includedInDataCatalog": raw_experiment.title,
                    "instrument": raw_dataset.instrument,
                    "name": raw_dataset.description,
                    "hasPart": [
                        {
                            "@id": (
                                ro_crate_dataset_dir / raw_datafile.filename
                            ).as_posix()
                        }
                    ],
                },
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "about": {"@id": "./"},
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                },
                {
                    "@id": raw_project.name,
                    "@type": "Project",
                    "name": raw_project.name,
                    "founder": raw_project.principal_investigator,
                    "url": raw_project.url,
                    "description": raw_project.description,
                },
                {
                    "@id": raw_experiment.title,
                    "@type": "DataCatalog",
                    "name": "#experiment_name",
                    "project": raw_project.name,
                    "description": raw_experiment.description,
                },
                {
                    "@id": (ro_crate_dataset_dir / raw_datafile.filename).as_posix(),
                    "@type": ["File"],
                    "name": (ro_crate_dataset_dir / raw_datafile.filename).as_posix(),
                    "contentSize": raw_datafile.size,
                    "encodingFormat": raw_datafile.mimetype,
                    "md5sum": raw_datafile.md5sum,
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


@pytest.fixture(name="fakecrate_root")
def fakecrate_root() -> Path:
    return Path("fake_ro_crate")


@pytest.fixture(name="fixture_fake_ro_crate")
def fixture_fake_ro_crate(
    tmp_path: Path,
    tmp_path_factory: pytest.TempPathFactory,
    test_rocrate_content: str,
    fakecrate_root: Path,
    rocrate_profile_json: str,
    raw_datafile: RawDatafile,
    ro_crate_dataset_dir: Path,
) -> None:
    crate_root = tmp_path / fakecrate_root / "data/"
    crate_root.mkdir(parents=True, exist_ok=True)
    with open(crate_root / "ro-crate-metadata.json", "w", encoding="utf-8") as f:
        f.write(test_rocrate_content)

    dataset_path = crate_root / ro_crate_dataset_dir
    dataset_path.mkdir(parents=True, exist_ok=True)
    with open(dataset_path / raw_datafile.filename, "w", encoding="utf-8") as f:
        f.write("size > 0")
    with open(dataset_path / "unlisted_file.txt", "w", encoding="utf-8") as f:
        f.write("size > 0")
    return crate_root


@pytest.fixture()
def fixture_rocrate_parser(
    fixture_fake_ro_crate: Path, fakecrate_root: Path
) -> ROCrateParser:
    return ROCrateParser(fixture_fake_ro_crate)

# pylint: disable=missing-docstring
# nosec assert_used
from pathlib import Path

import src.utils.filesystem.filters as filters
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.extraction.manifest import IngestionManifest
from src.profiles.ro_crate.ro_crate_parser import ROCrateParser
from tests.fixtures.fixtures_ro_crate import (
    fakecrate_root,
    fixture_fake_ro_crate,
    fixture_ro_crate_name,
    fixture_rocrate_datafile,
    fixture_rocrate_dataset,
    fixture_rocrate_experiment,
    fixture_rocrate_parser,
    fixture_rocrate_project,
    fixture_rocrate_uuid,
    ro_crate_dataset_dir,
    ro_crate_unlisted_file_dir,
    rocrate_profile_json,
    test_rocrate_content,
)


def test_load_crate(
    fixture_rocrate_parser: ROCrateParser,
    fakecrate_root: Path,
    fixture_ro_crate_name: str,
    fixture_rocrate_uuid: str,
    ro_crate_unlisted_file_dir: Path,
) -> None:
    """Basic Test to load an entire RO-Crate from a fixture

    Args:
        fixture_rocrate_parser (ROCrateParser): a fixture of an already loaded parser
            (based on other fixtures).
        Test will fail if this parser fails to load.
        fakecrate_root (Path): The location of the RO-Crate in the fake filesystem
    """
    assert fixture_rocrate_parser.name == fixture_ro_crate_name
    assert fixture_rocrate_parser.uuid.hex == fixture_rocrate_uuid


def test_read_crate_dataobjects(
    fixture_rocrate_parser: ROCrateParser,
    ro_crate_unlisted_file_dir: Path,
    fixture_ro_crate_name: str,
    fixture_rocrate_uuid: str,
    fixture_rocrate_datafile: RawDatafile,
    fixture_rocrate_dataset: RawDataset,
) -> None:
    """Test to confrim the RO-Crate parser has traversed datafiles correctly
    and associated them with the correct datasets.
    Also confirms that datasets haved been parsed correctly.
    """
    ingestible_dfs: IngestionManifest = fixture_rocrate_parser.process_datasets(
        IngestionManifest(), filters.PathFilterSet(filter_system_files=True)
    )
    assert len([df.description for df in ingestible_dfs.get_datasets()]) == 2
    assert (
        len([(df.dataset, df.filename) for df in ingestible_dfs.get_datafiles()]) == 3
    )  # raw_datafile, ro_crate_metadata.json and unlistedfile.txt

    # test file traversal of datafiles
    df_dict: dict[str, RawDatafile] = {
        df.filename: df for df in ingestible_dfs.get_datafiles()
    }
    # RO-Crate parser applies a unique name to each dataset
    # (as each must be named based on it's relative path)
    crate_unique_name = fixture_rocrate_uuid + "/" + fixture_ro_crate_name
    testing_datafile = df_dict[fixture_rocrate_datafile.filename]

    assert testing_datafile.directory == fixture_rocrate_dataset.directory
    assert testing_datafile.dataset == (
        crate_unique_name + "/" + fixture_rocrate_datafile.dataset + "/"
    )

    # a file not explicityl listed in the RO-Crate should accurately list it's directory
    # ... and be associated with it's nearest dataset in the path
    unlisted_datafile = df_dict["unlisted_file.txt"]
    assert unlisted_datafile.directory == ro_crate_unlisted_file_dir
    assert testing_datafile.dataset == (
        crate_unique_name + "/" + fixture_rocrate_dataset.description + "/"
    )
    # The RO-crate metadata file should be associated with the root dataset at ./
    root_datafile = df_dict["ro-crate-metadata.json"]
    assert root_datafile.dataset == crate_unique_name + "/./"

    ds_dict: dict[str, RawDataset] = {
        ds.description: ds for ds in ingestible_dfs.get_datasets()
    }

    root_dataset = ds_dict[crate_unique_name + "/./"]
    assert root_dataset
    assert root_dataset.instrument == "dummy-ro-crate-meta"
    assert root_dataset.metadata
    assert root_dataset.metadata["test-ro-crate-Metadata"] == "test value"
    testing_dataset = ds_dict[
        crate_unique_name + "/" + (fixture_rocrate_dataset.directory).as_posix() + "/"
    ]
    assert testing_dataset.experiments == fixture_rocrate_dataset.experiments
    assert testing_dataset.instrument == fixture_rocrate_dataset.instrument


def test_parse_crate_project(
    fixture_rocrate_parser: ROCrateParser,
    fixture_rocrate_project: RawProject,
) -> None:
    ingestible_projects: IngestionManifest = fixture_rocrate_parser.process_projects(
        IngestionManifest()
    )
    assert len(ingestible_projects.get_projects()) == 1
    testing_project = ingestible_projects.get_projects()[0]
    assert (
        testing_project.principal_investigator
        == fixture_rocrate_project.principal_investigator
    )
    assert testing_project.url == fixture_rocrate_project.url
    assert testing_project.description == fixture_rocrate_project.description


def test_parse_crate_experiment(
    fixture_rocrate_parser: ROCrateParser,
    fixture_rocrate_experiment: RawExperiment,
) -> None:
    ingestible_projects: IngestionManifest = fixture_rocrate_parser.process_experiments(
        IngestionManifest()
    )
    experiments = ingestible_projects.get_experiments()
    assert len(experiments) == 1
    testing_experiment = experiments[0]
    assert testing_experiment.projects == fixture_rocrate_experiment.projects
    assert testing_experiment.title == testing_experiment.title

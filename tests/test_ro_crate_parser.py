# nosec assert_used
from pathlib import Path

from pyfakefs.fake_filesystem import FakeFilesystem

import src.utils.filesystem.filters as filters
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.extraction.ingestibles import IngestibleDataclasses
from src.profiles.ro_crate.ro_crate_parser import ROCrateParser
from tests.fixtures.fixtures_ro_crate import (
    fakecrate_root,
    fixture_fake_ro_crate,
    fixture_ingested_rocrate_project,
    fixture_rocrate_parser,
    fixture_rocrate_uuid,
    ro_crate_dataset_dir,
    rocrate_profile_json,
    test_rocrate_content,
)


def test_load_crate(
    fixture_rocrate_parser: ROCrateParser, fakecrate_root: Path
) -> None:
    """Basic Test to load an entire RO-Crate from a fixture

    Args:
        fixture_rocrate_parser (ROCrateParser): a fixture of an already loaded parser (based on other fixtures).
        Test will fail if this parser fails to load.
        fakecrate_root (Path): The location of the RO-Crate in the fake filesystem
    """
    assert fixture_rocrate_parser.crate_name == fakecrate_root.as_posix()
    # TODO add check for UUID after rebasing to main
    ingestible_projects: IngestibleDataclasses = (
        fixture_rocrate_parser.process_projects(IngestibleDataclasses())
    )
    assert len(ingestible_projects.get_projects()) == 1

    ingestible_exps: IngestibleDataclasses = fixture_rocrate_parser.process_experiments(
        IngestibleDataclasses()
    )
    assert len(ingestible_exps.get_experiments()) == 1


def test_read_crate_dataobjects(
    fixture_rocrate_parser: ROCrateParser,
    raw_datafile: RawDatafile,
    raw_dataset: RawDataset,
    fakecrate_root: Path,
) -> None:
    ingestible_dfs: IngestibleDataclasses = fixture_rocrate_parser.process_datasets(
        IngestibleDataclasses(), filters.PathFilterSet(filter_system_files=True)
    )
    assert len([df.description for df in ingestible_dfs.get_datasets()]) == 2
    assert (
        len([(df.dataset, df.filename) for df in ingestible_dfs.get_datafiles()]) == 3
    )  # raw_datafile, ro_crate_metadata.json and unlistedfile.txt

    df_dict: dict[str, RawDatafile] = {
        df.filename: df for df in ingestible_dfs.get_datafiles()
    }

    testing_datafile = df_dict[raw_datafile.filename]
    assert testing_datafile
    assert testing_datafile.directory == raw_dataset.directory
    unlisted_datafile = df_dict["unlisted_file.txt"]
    assert unlisted_datafile
    assert unlisted_datafile.directory == raw_dataset.directory
    root_datafile = df_dict["ro-crate-metadata.json"]
    assert root_datafile
    assert root_datafile.dataset == fakecrate_root.as_posix() + "/./"

    ds_dict: dict[str, RawDataset] = {
        ds.description: ds for ds in ingestible_dfs.get_datasets()
    }
    # assert len(ds_dict) == 5
    root_dataset = ds_dict[fakecrate_root.as_posix() + "/./"]
    assert root_dataset
    assert root_dataset.instrument == "dummy-ro-crate-meta"
    assert len(root_dataset.metadata) == 1
    assert root_dataset.metadata["test-ro-crate-Metadata"] == "test value"

    testing_dataset = ds_dict[(fakecrate_root / raw_dataset.directory).as_posix() + "/"]
    assert testing_dataset.experiments == raw_dataset.experiments
    assert testing_dataset.instrument == raw_dataset.instrument


def test_read_crate_project(
    fixture_rocrate_parser: ROCrateParser,
    raw_project: RawProject,
) -> None:
    ingestible_projects: IngestibleDataclasses = (
        fixture_rocrate_parser.process_projects(IngestibleDataclasses())
    )
    assert len(ingestible_projects.get_projects()) == 1
    testing_project = ingestible_projects.get_projects()[0]
    assert testing_project.principal_investigator == raw_project.principal_investigator
    assert testing_project.url == raw_project.url
    assert testing_project.description == raw_project.description


def test_read_crate_experiment(
    fixture_rocrate_parser: ROCrateParser,
    raw_experiment: RawExperiment,
) -> None:
    ingestible_projects: IngestibleDataclasses = (
        fixture_rocrate_parser.process_experiments(IngestibleDataclasses())
    )
    experiments = ingestible_projects.get_experiments()
    assert len(experiments) == 1
    testing_experiment = experiments[0]
    assert testing_experiment.projects == raw_experiment.projects
    assert testing_experiment.title == testing_experiment.title

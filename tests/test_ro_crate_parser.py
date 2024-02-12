# nosec assert_used
from pathlib import Path

from pyfakefs.fake_filesystem import FakeFilesystem

import src.utils.filesystem.filters as filters
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
    assert fixture_rocrate_parser.crate_name == fakecrate_root.parent.parts[-1]
    # TODO add check for UUID after rebasing to main
    ingestible_projects: IngestibleDataclasses = (
        fixture_rocrate_parser.process_projects(IngestibleDataclasses())
    )
    assert len(ingestible_projects.get_projects()) == 1
    ingestible_dfs: IngestibleDataclasses = fixture_rocrate_parser.process_datasets(
        IngestibleDataclasses(), filters.PathFilterSet(filter_system_files=True)
    )
    assert len(ingestible_dfs.get_datasets()) == 1
    assert len(ingestible_dfs.get_datafiles()) == 2
    ingestible_exps: IngestibleDataclasses = fixture_rocrate_parser.process_experiments(
        IngestibleDataclasses()
    )
    assert len(ingestible_exps.get_experiments()) == 1

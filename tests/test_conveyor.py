"""test_conveyor.py - Tests for conveyor and transports"""

import os
import random
from pathlib import Path
from typing import Callable

import pytest

from src.blueprints.datafile import Datafile, DatafileReplica
from src.config.config import FilesystemStorageBoxConfig
from src.conveyor.conveyor import Conveyor, is_rsync_on_path
from src.mytardis_client.endpoints import URI
from src.utils.filesystem.checksums import calculate_md5

DatafileFixture = tuple[Path, list[Datafile], Path]


@pytest.fixture(name="datafile_list")
def fixtures_datafile_list(tmp_path: Path) -> Callable[[int], DatafileFixture]:
    def _fixtures_datafile_list(
        dataset_id: int, num_files: int = 15
    ) -> DatafileFixture:
        """Generates a temporary directory with random data files,
        and source and destination folders.

        Args:
            tmp_path (Path): pytest's temporary directory fixture.

        Returns:
            tuple[Path, list[BaseDatafile], Path]: A tuple with three elements,
                - the path for source directory
                - a list of random datafiles
                - the path for destination directory
        """
        # Create source and dest directories
        src_dir = tmp_path / "data"
        dest_dir = tmp_path / "destination"
        src_dir.mkdir(exist_ok=True)
        dest_dir.mkdir(exist_ok=True)
        datafiles: list[Datafile] = []
        for i in range(num_files):
            # Generate 15 random files
            f_path = src_dir / (str(i) + ".txt")
            f_path.touch()
            # Add some random data.
            with f_path.open("wb") as f:
                f.write(random.randbytes(100))
            # Calculate the md5sum.
            digest = calculate_md5(f_path)
            # Create a datafile.
            df = Datafile(
                filename=f_path.name,
                directory=Path(""),
                md5sum=digest,
                mimetype="text/plain",
                size=f_path.stat().st_size,
                dataset=URI(f"/api/v1/dataset/{dataset_id}/"),
                replicas=[
                    DatafileReplica(
                        uri=f"{dest_dir}/dataset/{f_path.name}", location="test-box"
                    )
                ],
            )
            datafiles.append(df)
        return (src_dir, datafiles, dest_dir)

    return _fixtures_datafile_list


@pytest.mark.skipif(not is_rsync_on_path(), reason="requires rsync installed")
def test_rsync_transfer_ok(datafile_list: Callable[[int], DatafileFixture]) -> None:
    """Test for an rsync transfer - the process should exit without error
    and the files should be replicated.

    Args:
        datafile_list (DatafileFixture): Datafile fixtures.
    """
    src, dfs, dest = datafile_list(409)
    store = FilesystemStorageBoxConfig(storage_name="test-box", target_root_dir=dest)
    conveyor = Conveyor(store)
    # Run the transfer
    conveyor.transfer(src, dfs)
    src_files = os.listdir(src)
    # Ensure that a directory for the dataset is created.
    dataset_dir = dest / "ds-409"
    assert dataset_dir.is_dir()
    dest_files = os.listdir(dataset_dir)
    # There should be the same number of files at destination.
    assert sorted(dest_files) == sorted(src_files)


@pytest.mark.skipif(not is_rsync_on_path(), reason="requires rsync installed")
def test_rsync_transfer_single_file_ok(
    datafile_list: Callable[[int, int], DatafileFixture]
) -> None:
    """Test for rsync transferring one file should complete, and file should be inside a directory.

    Args:
        datafile_list (Callable[[int], DatafileFixture]): Datafile fixtures
    """
    src, dfs, dest = datafile_list(401, 1)
    store = FilesystemStorageBoxConfig(storage_name="test-box", target_root_dir=dest)
    conveyor = Conveyor(store)
    # Run the transfer
    conveyor.transfer(src, dfs)
    src_files = os.listdir(src)
    # Ensure that a directory for the dataset is created.
    dataset_dir = dest / "ds-401"
    assert dataset_dir.is_dir()
    dest_files = os.listdir(dataset_dir)
    # There should be the same number of files at destination.
    assert sorted(dest_files) == sorted(src_files)


@pytest.mark.skipif(not is_rsync_on_path(), reason="requires rsync installed")
def test_rsync_transfer_multiple_datasets(
    datafile_list: Callable[[int], DatafileFixture]
) -> None:
    """Test to ensure an ingestion with multiple datasets are transferred to separate datasets.

    Args:
        datafile_list (Callable[[int], DatafileFixture]): The generated datafile.
    """
    # Create files with different datasets
    src, first_dfs, dest = datafile_list(1)
    _, second_dfs, _ = datafile_list(2)
    combined_dfs = first_dfs + second_dfs
    # Perform the transfer
    store = FilesystemStorageBoxConfig(storage_name="test-box", target_root_dir=dest)
    conveyor = Conveyor(store)
    conveyor.transfer(src, combined_dfs)
    # Check each dataset folder to ensure they have all the datafiles.
    assert (dest / "ds-1").is_dir()
    dsone_dest_files = os.listdir(dest / "ds-1")
    assert sorted(dsone_dest_files) == sorted([df.filename for df in first_dfs])
    assert (dest / "ds-2").is_dir()
    dstwo_dest_files = os.listdir(dest / "ds-2")
    assert sorted(dstwo_dest_files) == sorted([df.filename for df in second_dfs])

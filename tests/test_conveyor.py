"""test_conveyor.py - Tests for conveyor and transports"""
import os
import random
from pathlib import Path

import pytest

from src.blueprints.datafile import BaseDatafile, RawDatafile
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport, is_rsync_on_path
from src.miners.utils.datafile_metadata_helpers import calculate_md5sum

DatafileFixture = tuple[Path, list[BaseDatafile], Path]


@pytest.fixture(name="datafile_list")
def fixtures_datafile_list(tmp_path: Path) -> DatafileFixture:
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
    src_dir.mkdir()
    dest_dir.mkdir()
    datafiles: list[BaseDatafile] = []
    for i in range(15):
        # Generate 15 random files
        f_path = src_dir / (str(i) + ".txt")
        f_path.touch()
        # Add some random data.
        with f_path.open("wb") as f:
            f.write(random.randbytes(100))
        # Calculate the md5sum.
        digest = calculate_md5sum(f_path)
        # Create a datafile.
        df = RawDatafile(
            filename=f_path.name,
            directory=Path(""),
            md5sum=digest,
            mimetype="text/plain",
            size=f_path.stat().st_size,
            dataset="dataset",
        )
        datafiles.append(df)
    return (src_dir, datafiles, dest_dir)


@pytest.mark.skipif(not is_rsync_on_path(), reason="requires rsync installed")
def test_rsync_transfer_ok(datafile_list: DatafileFixture) -> None:
    """Test for an rsync transfer - the process should exit without error
    and the files should be replicated.

    Args:
        datafile_list (DatafileFixture): Datafile fixtures.
    """
    src, dfs, dest = datafile_list
    rsync = RsyncTransport(dest)
    conveyor = Conveyor(rsync)
    process = conveyor.initiate_transfer(src, dfs)
    # Wait for transfer to finish
    process.join()
    # Process should exit normally
    assert process.exitcode == 0
    src_files = os.listdir(src)
    dest_files = os.listdir(dest)
    # There should be the same number of files at destination.
    assert sorted(dest_files) == sorted(src_files)

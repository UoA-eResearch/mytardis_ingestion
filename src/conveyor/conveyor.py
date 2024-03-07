"conveyor.py - Script for file transferring."

import logging
import shutil
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

from src.blueprints.datafile import Datafile, DatafileReplica
from src.config.config import FilesystemStorageBoxConfig
from src.overseers.overseer import Overseer

logger = logging.getLogger(__name__)


class FailedTransferException(Exception):
    """A custom exception for transfer failures."""


def is_rsync_on_path() -> bool:
    """Tests whether rsync is available on the PATH.

    Returns:
        bool: True if rsync is found, false if not.
    """
    rsync_path = shutil.which("rsync")
    return rsync_path is not None


class Conveyor:
    """Class for transferring datafiles as part of ingestion pipeline.
    Currently, Conveyor supports using rsync to transfer files
    on the filesystem. Other mechanisms and storage types can be supported
    by extracting an interface from the conveyor."""

    def __init__(self, store: FilesystemStorageBoxConfig) -> None:
        """Initialises the file transfer Conveyor object.

        Args:
            transport (AbstractTransport): The concrete transport mechanism to use.
        """
        if not is_rsync_on_path():
            raise RuntimeError("Could not find rsync on PATH.")
        self._store = store

    def create_replica(self, file: Datafile) -> DatafileReplica:
        """Method for creating a DatafileReplica representing the files copied by this
        conveyor. This method does not transfer the files, it merely creates an object
        representing the copied files in the StorageBox. Use Conveyor.transfer()
        to transfer the files.

        Args:
            file (Datafile): The file to create a Replica representation of.

        Returns:
            DatafileReplica: The Replica representing the copied file.
        """
        file_path = file.directory / file.filename
        dataset_id = Overseer.resource_uri_to_id(file.dataset)
        return DatafileReplica(
            protocol="file",
            location=self._store.storage_name,
            uri=f"ds-{dataset_id}/{file_path.as_posix()}",
        )

    def _transfer_with_rsync(
        self, src: Path, files: list[Path], destination_dir: Path
    ) -> None:
        """Private method for transferring the files using rsync.

        Args:
            src (Path): Root source directory.
            files (list[Path]): A list of file Paths relative to the root directory.
            destination_dir (Path): The destination directory.

        Raises:
            FailedTransferException: Raised when rsync does not return with status code 0.
        """
        with NamedTemporaryFile("r+") as list_f:
            # Write to temporary file list for rsync to sync over.
            for path in files:
                list_f.write(str(path) + "\n")
            list_f.flush()
            # Run rsync commandline to transfer the files.
            # Disable bandit's warning about subprocess.
            logger.info("rsync process has started, output not available until it finishes.")
            result = subprocess.run(  # nosec
                ["rsync", "-av", "--files-from", list_f.name, src, destination_dir],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            logger.info("rsync process finished. Output below.")
            logger.info(result.stdout.decode())
            if result.returncode > 0:
                raise FailedTransferException("rsync return code was not 0.")

    def transfer(self, data_root: Path, dfs: list[Datafile]) -> None:
        """Initiates a transfer and blocks until it returns. The files will be transferred
        to the configured StorageBox, and separated based on the dataset it belongs to.
        The path of the file will be [storagebox uri]/ds-[dataset id]/[directory]/[filename].

        Raises:
            FailedTransferException: Raised if transport encounters an error
            during transfer.

        Args:
            src (Path): Path of the source directory.
            dfs (list[Datafile]): List of Datafiles to transfer.

        Returns:
            None.
        """
        files_by_dataset: dict[int, list[Datafile]] = {}
        for df in dfs:
            # Group datafiles by the dataset they are in.
            dataset_id = Overseer.resource_uri_to_id(df.dataset)
            if dataset_id not in files_by_dataset:
                files_by_dataset[dataset_id] = []
            files_by_dataset[dataset_id].append(df)
        for dataset_id, file_list in files_by_dataset.items():
            # For each group of datafiles, transfer to a separate folder.
            file_paths = [df.directory / df.filename for df in file_list]
            destination_dir = self._store.target_root_dir / f"ds-{dataset_id}"
            self._transfer_with_rsync(data_root, file_paths, destination_dir)

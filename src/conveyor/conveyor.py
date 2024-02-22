"conveyor.py - Script for file transferring."

import logging
import multiprocessing as mp
from multiprocessing.context import SpawnProcess
from pathlib import Path
from typing import Sequence

from src.blueprints.datafile import Datafile, DatafileReplica
from src.config.config import FilesystemStorageBoxConfig, StorageBoxConfig
from src.conveyor.transports.common import AbstractTransport, TransferObject
from src.conveyor.transports.rsync import RsyncTransport

logger = logging.getLogger(__name__)
class Conveyor:
    """Class for transferring datafiles as part of ingestion pipeline."""

    transport: AbstractTransport

    def _get_transport_by_store(self, store: StorageBoxConfig) -> AbstractTransport:
        if not isinstance(store, FilesystemStorageBoxConfig):
            raise NotImplementedError("StorageBox class is not supported.")
        # Create a transport for filesystem storagebox
        return RsyncTransport(store.target_root_dir)

    def __init__(self, store: StorageBoxConfig) -> None:
        """Initialises the file ingestion object.

        Args:
            transport (AbstractTransport): The concrete transport mechanism to use.
        """
        self._store = store
        self._transport = self._get_transport_by_store(store)

    def transfer_blocking(self, src: Path, dfs: list[Datafile]) -> None:
        """Initiates a transfer via specified transport and
        blocks until it returns.

        Raises:
            FailedTransferException: Raised if transport encounters an error
            during transfer.

        Args:
            src (Path): Path of the source directory.
            dfs (list[BaseDatafile]): List of Datafiles to transfer.

        Returns:
            None.
        """
        files_to_transfer: list[TransferObject] = []
        for f in dfs:
            if len(f.replicas) == 0:
                logger.error("No replica was created for %s. File will not be transferred",
                    f.filename)
                continue
            if len(f.replicas) > 1:
                logger.warning(
                    "More than one replica was created in the forged Datafile. " +
                    "Conveyor will only transfer the first replica."
                )
            replica = f.replicas[0]
            if replica.location != self._store.storage_name:
                logger.error(
                    "Replica for %s is not intended for the storagebox configured for the conveyor."
                    , f.filename
                )
                continue
            files_to_transfer.append(TransferObject(f.directory, f.filename, replica.uri))
        self._transport.transfer(src, files_to_transfer)

    def initiate_transfer(self, src: Path, dfs: Sequence[Datafile]) -> SpawnProcess:
        """Spawns a separate Process to transfer via specified transport and
        returns the Process. Client code should store this reference,
        perform the rest of the metadata ingestion operations,
        then call `.join()` to wait for the file transfer to finish.

        Args:
            src (Path): Path of the source directory.
            dfs (list[BaseDatafile]): List of Datafiles to transfer.

        Returns:
            SpawnProcess: The spawned process. Call process.join() to wait for transfer
            to complete.
        """
        ctx = mp.get_context("spawn")
        process = ctx.Process(
            target=self.transfer_blocking, kwargs={"src": src, "dfs": dfs}
        )
        process.start()
        return process

"conveyor.py - Script for file transferring."

import logging
import multiprocessing as mp
from multiprocessing.context import SpawnProcess
from pathlib import Path
from typing import Sequence

from src.blueprints.datafile import Datafile, DatafileReplica, RawDatafile
from src.blueprints.storage_boxes import StorageTypesEnum
from src.config.config import StorageBoxConfig
from src.conveyor.transports.common import AbstractTransport
from src.conveyor.transports.rsync import RsyncTransport

logger = logging.getLogger(__name__)
class Conveyor:
    """Class for transferring datafiles as part of ingestion pipeline."""

    transport: AbstractTransport

    def _get_transport_by_store(self, store: StorageBoxConfig) -> AbstractTransport:
        match store.storage_class:
            case StorageTypesEnum.FILE_SYSTEM:
                if store.options is None:
                    raise ValueError()
                dest_path = Path(store.options["target_root_directory"])
                return RsyncTransport(dest_path)
            case StorageTypesEnum.S3:
                raise NotImplementedError()

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
        filepaths = 
        
        replicas: list[DatafileReplica] = []
        for f in dfs:
            
            replicas += f.replicas
        for replica in replicas:
            # Check through the replicas.
            if not replica.location == self._store.storage_name:
                logger.error("Replica %s is intended for a different" + 
                    " storage box than the one configured for conveyor.",replica.uri)
                continue
            
        self._transport.transfer(src, files)

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

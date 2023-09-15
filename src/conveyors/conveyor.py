import multiprocessing as mp
from multiprocessing.context import SpawnProcess
from pathlib import Path
from typing import Sequence

from src.blueprints.datafile import BaseDatafile
from src.config.singleton import Singleton
from src.conveyors.transports.common import AbstractTransport


class Conveyor(metaclass=Singleton):
    """Class for transferring datafiles as part of ingestion pipeline."""

    def __init__(self, transport: AbstractTransport) -> None:
        """Initialises the file ingestion object.

        Args:
            transport (AbstractTransport): The concrete transport mechanism to use.
        """
        self.transport = transport

    def _generate_file_list(self, dfs: list[BaseDatafile]) -> list[Path]:
        """Generates a list of paths relative to a source directory for datafiles.

        Returns:
            list[Path]: List of relative paths for files.
        """
        return [file.directory / file.filename for file in dfs]

    def initiate_transfer_blocking(self, src: Path, dfs: list[BaseDatafile]) -> None:
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
        files = self._generate_file_list(dfs)
        self.transport.transfer(src, files)

    def initiate_transfer(self, src: Path, dfs: Sequence[BaseDatafile]) -> SpawnProcess:
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
            target=self.initiate_transfer_blocking, kwargs={"src": src, "dfs": dfs}
        )
        process.start()
        return process

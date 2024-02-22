"""common.py - common transport-related class and interface."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from src.blueprints.datafile import Datafile, DatafileReplica


class FailedTransferException(Exception):
    """A custom exception for transfer failures."""

@dataclass
class TransferObject:
    """Dataclass to store information related to transferring a file.
    """
    src_directory: Path
    src_filename: str
    destination_uri: str

class AbstractTransport(ABC):
    """Abstract class for different ways of transferring
    files.
    """

    @abstractmethod
    def transfer(self, src: Path, files: list[TransferObject]) -> None:
        """Abstract method that does the file transfer.
        Given a list of files paths, transfers to
        a configured destination.

        Raises:
            FailedTransferException: When an error happens during
            transfer.

        Args:
            src (Path): Path to the source directory
            files (list[Datafile]): A list of Datafiles 
                to transfer. Source paths are assumed to be 
                relative to `src`.
        """
